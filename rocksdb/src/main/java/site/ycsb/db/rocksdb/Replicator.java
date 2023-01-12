package site.ycsb.db.rocksdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import site.ycsb.*;
import site.ycsb.RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub;
import site.ycsb.RubbleKvStoreServiceGrpc.RubbleKvStoreServiceBlockingStub;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
// import java.util.Arrays;
import java.sql.Timestamp;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.locks.*;

/**
 * Replicator in chain replication.
 *
 * @author Haoyu Li.
 */
public class Replicator {
  private final int shardNum;
  private final int clientNum;
  private static Properties props;
  private final int port;
  private final Server server;
  private final ManagedChannel[] headChannel;
  private final ManagedChannel[] tailChannel;
  private static RubbleKvStoreServiceStub[] headStub; // added static
  private static RubbleKvStoreServiceStub[] tailStub;
  private final StreamObserver[][][] observerMap; // keys are [shardIdx][clientIdx][isWrite]
  private final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(16);
  private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

  // HEARTBEAT
  private static List<List<ManagedChannel>> channels;
  private static int[] replicationFactor;
  private static List<List<RubbleKvStoreServiceBlockingStub>> healthStub;
  private static long needRestart = 0;
  private final LongAccumulator opssent = new LongAccumulator(Long::sum, 0L); 
  private final LongAccumulator maxThreads = new LongAccumulator(Long::sum, 0L);
  // HEARTBEAT

  // RECOVERY
  private static final long FAILED_OP_WATERMARK = 5000000L;
  private final RecoveryHelper recoveryHelper;
  private final LongAccumulator opsHandled = new LongAccumulator(Long::sum, 0L);
  private static Lock recoveryProcessLock = new ReentrantLock();
  private static boolean doFail = true;
  // RECOVERY

  private final class RecoveryHelper {
    private String tailToKillHost;
    private RubbleKvStoreServiceBlockingStub headBlockingStub;

    private RecoveryHelper(String tailHost, RubbleKvStoreServiceBlockingStub stub) {
      this.tailToKillHost = tailHost;
      this.headBlockingStub = stub;
    }

    public void executeCommands(List<String> commands) {
      try {
        ProcessBuilder pb = new ProcessBuilder(commands);
        Process process = pb.start();
        LOGGER.info("execute command: " + String.join(" ", commands.toArray(String[]::new)));

        StringBuilder out = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null, previous = null;
        while ((line = br.readLine()) != null) {
          if (!line.equals(previous)) {
            previous = line;
            out.append(line).append('\n');
          }
        }
        if (!out.toString().isBlank()) {
          LOGGER.info(out.toString());
        }

        //Check result
        if (process.waitFor() != 0) {
          out = new StringBuilder();
          br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
          while ((line = br.readLine()) != null) {
            if (!line.equals(previous)) {
              previous = line;
              out.append(line).append('\n');
            }
          }
          if (!out.toString().isBlank()) {
            LOGGER.error("error: " + out.toString());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    public void killTail() {
      LOGGER.info("killing the tail node...");
      List<String> killCommand = Arrays.asList("sudo", "killall", "db_node");
      List<String> commands = new ArrayList<String>(Arrays.asList("bash", "-c",
          String.format("ssh %s \"%s\"", tailToKillHost, String.join(" ", killCommand))));
      executeCommands(commands);
      LOGGER.info("tail node is killed!");
    }

    public void restartTail() {
      LOGGER.info("restarting the tail node...");
      List<String> runCommands = Arrays.asList(
          "cd /mnt/data/my_rocksdb/rubble;",
          "ulimit -n 999999;",
          "ulimit -c unlimited;",
          "sudo killall db_node dstat iostat perf top > /dev/null 2>&1;",
          "sudo bash clean.sh 1 > /dev/null 2>&1;",
          "nohup sudo " +
          // cgexec -g cpuset:rubble-cpu -g memory:rubble-mem 
          "./db_node 50050 10.10.1.1:50040 0 2 3 > shard-0.out 2>&1 &"
      );
      List<String> commands = new ArrayList<String>(Arrays.asList("bash", "-c",
          String.format("ssh %s \"%s\"", tailToKillHost, String.join(" ", runCommands))));
      executeCommands(commands);
      LOGGER.info("tail node restarted");
    }

    public void headFailOver() {
      // head doesn't need to wait for tail db_node restarts
      // it can immediately re-ship SSTs
      assert(headStub[0] != null);
      
      FailOverRequest request = FailOverRequest.newBuilder().build();
      LOGGER.info("start failover the head...");
      FailOverReply reply = headBlockingStub.headFailOver(request);
      LOGGER.info("failover the head done");
    }
  }
  
  public Replicator() throws IOException {
    this.shardNum = Integer.parseInt(props.getProperty("shard"));
    this.clientNum = Integer.parseInt(props.getProperty("client"));
    String[] headNode = new String[shardNum];
    String[] tailNode = new String [shardNum];
    this.headChannel = new ManagedChannel[shardNum];
    this.tailChannel = new ManagedChannel[shardNum];
    this.headStub = new RubbleKvStoreServiceStub[shardNum];
    this.tailStub = new RubbleKvStoreServiceStub[shardNum];
    this.healthStub = new ArrayList<>(shardNum);
    this.observerMap = new StreamObserver[shardNum][clientNum][2];
    this.port = Integer.parseInt(props.getProperty("port"));
    ServerBuilder serverBuilder = ServerBuilder.forPort(port).addService(new RubbleKvStoreService());
    this.server = serverBuilder.build();
    // HEARTBEAT
    // this.replicationFactor = new int[shardNum];
    // int replicaPerChain = Integer.parseInt(props.getProperty("replica", "3"));
    // Arrays.fill(this.replicationFactor, replicaPerChain); // TODO: this is hard-coded
    // this.healthStub = new ArrayList<>(shardNum);
    // this.channels = new ArrayList<>(shardNum);
    // HEARTBEAT
    for (int i = 0; i < shardNum; i++) {
      headNode[i] = props.getProperty("head"+i);
      tailNode[i] = props.getProperty("tail"+i);
      
      this.headChannel[i] = ManagedChannelBuilder.forTarget(headNode[i]).usePlaintext().build();
      this.tailChannel[i] = ManagedChannelBuilder.forTarget(tailNode[i]).usePlaintext().build();
      this.headStub[i] = RubbleKvStoreServiceGrpc.newStub(this.headChannel[i]);
      this.tailStub[i] = RubbleKvStoreServiceGrpc.newStub(this.tailChannel[i]);
      // HEARTBEAT
      // this.healthStub.add(new ArrayList<RubbleKvStoreServiceBlockingStub>(replicationFactor[i]));
      // this.channels.add(new ArrayList<ManagedChannel>(replicationFactor[i]));
      // for(int j = 0; j < replicationFactor[i]; j++) {
      //   // TODO: temporary fix on channel in 2 & 3 -node setup
      //   if (j == 0) {
      //     this.channels.get(i).add(headChannel[i]);
      //     this.healthStub.get(i).add(RubbleKvStoreServiceGrpc.newBlockingStub(this.headChannel[i]));
      //   } else if (j == replicationFactor[i]-1) {
      //     this.channels.get(i).add(tailChannel[i]);
      //     this.healthStub.get(i).add(RubbleKvStoreServiceGrpc.newBlockingStub(this.tailChannel[i]));
      //   } else { // middle node
      //     String middleNode = props.getProperty("middle"+(i+1)+"_"+j); // TMP FIX
      //     ManagedChannel middleChan = ManagedChannelBuilder.forTarget(middleNode).usePlaintext().build(); 
      //     this.channels.get(i).add(middleChan);
      //     this.healthStub.get(i).add(RubbleKvStoreServiceGrpc.newBlockingStub(middleChan));
      //   }
      // }
      // HEARTBEAT
    }
    this.recoveryHelper = new RecoveryHelper(tailNode[0].split(":")[0],
        RubbleKvStoreServiceGrpc.newBlockingStub(this.headChannel[0]));
  }

  public void start() throws IOException {
    server.start();
    LOGGER.info("Server started, listening on " + port);
    // start heartbeat
    // Thread thread = new Thread(new Ping());
    // thread.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          Replicator.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        System.err.println("*** server shut down");
      }
    });
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      threadPoolExecutor.shutdownNow();
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
    for (int i = 0; i < shardNum; i++) {
      if (headChannel[i] != null) {
        headChannel[i].shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
      if (tailChannel[i] != null) {
        tailChannel[i].shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
  
  public static void main(String[] args) throws Exception {
    //LOGGER.info("Replicator started");
    props = Client.parseArguments(args);
    Replicator server = new Replicator();
    server.start();
    server.blockUntilShutdown();
  }

  private class RubbleKvStoreService extends RubbleKvStoreServiceGrpc.RubbleKvStoreServiceImplBase {
    private final LongAccumulator observerAccumulator = new LongAccumulator(Long::sum, 0L);

    RubbleKvStoreService() {}

    @Override
    public StreamObserver<OpReply> sendReply(final StreamObserver<Reply> responseObserver) {
      return new StreamObserver<OpReply>() {
        private boolean isWrite;
        private int shardIdx = -1;
        private int clientIdx = -1;
        private int isWriteInt = -1;

        private String logString(String prefix) {
          Timestamp timestamp = new Timestamp(System.currentTimeMillis());
          return prefix + " shard: " + shardIdx + " client: " + clientIdx + " write:" + isWrite + " " + timestamp;
        }

        @Override
        public void onNext(OpReply reply) {
          if (shardIdx == -1) {
            shardIdx   = reply.getShardIdx();
            clientIdx  = reply.getClientIdx();
            isWrite    = reply.getReplies(0).getType() != OpType.GET &&
              reply.getReplies(0).getType() != OpType.SCAN;
            isWriteInt = isWrite ? 1 : 0;
          }
          assert(shardIdx  == reply.getShardIdx());
          assert(clientIdx == reply.getClientIdx());
          assert(isWrite   == (reply.getReplies(0).getType() != OpType.GET &&
            reply.getReplies(0).getType() != OpType.SCAN));
          observerMap[shardIdx][clientIdx][isWriteInt].onNext(reply);
        }

        @Override
        public void onError(Throwable t) {
          System.out.println(logString("sendReply.onError"));
          LOGGER.error("Encountered error in sendReply", t);
        }

        @Override
        public void onCompleted() {
          observerMap[shardIdx][clientIdx][isWriteInt].onCompleted();
          responseObserver.onCompleted();
          System.out.println(logString("sendReply.onCompleted"));
        }
      };
    }

    @Override
    public StreamObserver<Op> doOp(final StreamObserver<OpReply> responseObserver) {
      return new StreamObserver<Op>() {
        private boolean isWrite;
        private int shardIdx = -1;
        private int clientIdx = -1;
        private int isWriteInt = -1;

        private StreamObserver<Op> headObserver = null;
        private StreamObserver<Op> tailObserver = null;
        private StreamObserver<OpReply> headReplyObserver = null;
        private StreamObserver<OpReply> tailReplyObserver = null;
        private ClientCallStreamObserver<Op> headCallObserver = null;
        private ClientCallStreamObserver<Op> tailCallObserver = null;

        private String logString(String prefix) {
          Timestamp timestamp = new Timestamp(System.currentTimeMillis());
          return prefix + " shard: " + shardIdx + " client: " + clientIdx + " write:" + isWrite + " " + timestamp;
        }

        private void buildObserver(boolean isHead) {
          StreamObserver<OpReply> observer = new StreamObserver<OpReply>() {
              private int shard = shardIdx;
              private int client = clientIdx;
              private boolean write = isHead;

              private String logString(String prefix) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                return prefix + " shard: " + shard + " client: " + client + " write:" + write + " " + timestamp;
              }
                          
              @Override
              public void onNext(OpReply reply) {
                LOGGER.error("[dumbObserver.onNext] should not reach here");
              }

              @Override
              public void onError(Throwable t) {
                System.out.println(logString("dumbObserver.onError"));
              }

              @Override
              public void onCompleted() {
                System.out.println(logString("dumbObserver.onCompleted"));
              }
          };

          if (isHead) {
            headReplyObserver = observer;
          } else {
            tailReplyObserver = observer;
          }
        }

        @Override
        public void onNext(Op request) {
          if (shardIdx == -1) {
            shardIdx   = request.getShardIdx();
            clientIdx  = request.getClientIdx();
            isWrite    = request.getOps(0).getType() != OpType.GET &&
              request.getOps(0).getType() != OpType.SCAN;
            isWriteInt = isWrite ? 1 : 0;
          }
          assert(shardIdx  == request.getShardIdx());
          assert(clientIdx == request.getClientIdx());
          assert(isWrite   == (request.getOps(0).getType() != OpType.GET &&
            request.getOps(0).getType() != OpType.SCAN));

          if (responseObserver != observerMap[shardIdx][clientIdx][isWriteInt]) {
            System.out.println("observerMap[" + shardIdx + "] " + "[" + clientIdx + "] " + "[" + isWriteInt + "] " +
                "changes from " + observerMap[shardIdx][clientIdx][isWriteInt] + " to " + responseObserver);
            observerMap[shardIdx][clientIdx][isWriteInt] = responseObserver;
          }

          if (!isWrite) {
            if (tailObserver == null) {
              buildObserver(isWrite);
              tailObserver = tailStub[shardIdx].doOp(tailReplyObserver);
              tailCallObserver = (ClientCallStreamObserver<Op>)tailObserver;
              observerAccumulator.accumulate(1);
              if (observerAccumulator.longValue() > maxThreads.longValue()) {
                maxThreads.accumulate(1);
              }
              System.out.println("observerAccumulator " + observerAccumulator.longValue() + " shard " + shardIdx);
            }
            while (!tailCallObserver.isReady()) {
              try {
                Thread.sleep(1);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            tailObserver.onNext(request);
          } else {
            if (headObserver == null) {
              buildObserver(isWrite);
              headObserver = headStub[shardIdx].doOp(headReplyObserver);
              headCallObserver = (ClientCallStreamObserver<Op>)headObserver;
              observerAccumulator.accumulate(1);
              if (observerAccumulator.longValue() > maxThreads.longValue()) {
                maxThreads.accumulate(1);
              }
              System.out.println("observerAccumulator " + observerAccumulator.longValue() + " shard " + shardIdx);
            }

            if (needRestart > 0) {
              buildObserver(true);
              headObserver = headStub[shardIdx].doOp(headReplyObserver);
              needRestart--;
              System.out.println("Restarting head observer");
            }

            while (!headCallObserver.isReady()) {
              try {
                Thread.sleep(1);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            headObserver.onNext(request);
            opssent.accumulate(request.getOpsCount());
          }
          // recovery:
          recoveryProcessLock.lock();
          if (doFail) {
            opsHandled.accumulate(request.getOpsCount());
            LOGGER.info("current ops handled: " + opsHandled.get());
            if (opsHandled.get() >= FAILED_OP_WATERMARK) {
              recoveryHelper.killTail();
              recoveryHelper.restartTail();
              recoveryHelper.headFailOver();
              doFail = false;
            }
          }
          recoveryProcessLock.unlock();

          // also send termination message to the head to clean all buffered requests in
          // secondaries because read threads don't buffer anything
          if (request.getId() == -1) {
            if (request.getOps(0).getType() == OpType.GET && headObserver != null) {
              synchronized (headObserver) {
                headObserver.onNext(request);
              }
            } 
          }
        }

        @Override
        public void onError(Throwable t) {
          System.out.println(logString("doOp.onError"));
          LOGGER.error("Encountered error in doOp", t);
        }

        @Override
        public void onCompleted() {
          if (tailObserver != null) {
            tailObserver.onCompleted();
            System.out.println(logString("tailObserver.onCompleted"));
          }
          if (headObserver != null) {
            headObserver.onCompleted();
            System.out.println(logString("headObserver.onCompleted"));
          }
        }
      };
    }
  }

  private class Ping implements Runnable {
    private final int deadlineMs = 1000000; // [TODO] (cc4351) parameterize this
    public void run() {
    // TODO: a better defined heart-beat frequency and deadline for RPC
      int wait = 0;
      while(true) {
        for(int i = 0; i < shardNum; i++) {
          for(int j = 0; j < replicationFactor[i]; j++) {
            try {
              LOGGER.info("[i]: " + i + ", [j]: " + j + " wait: " + wait + " :,)");
              pulse(false, false, i, j);
              Thread.sleep(500);
            } catch(InterruptedException e) {
              LOGGER.error("ping thread interrupted");
            } catch (StatusRuntimeException e) {
              onError(e, i, j);
            }
          }
          wait++;
        }
      }
    }

    private void onError(StatusRuntimeException e, int shardId, int nodeId) {
      LOGGER.error("ping failure on shard: " + shardId + ", node: " + nodeId + " " + e.toString());
      // check which node failed
      if (nodeId == 0) {
        String currentTime = String.format("%1$TH:%1$TM:%1$TS", System.currentTimeMillis());
        LOGGER.error("head failure at " + currentTime + " recovering with remaining "
                      + (replicationFactor[shardId] - 1) + " nodes....");
        // replicationFactor updated
        if (--replicationFactor[shardId] <= 0) {
          LOGGER.error("violating assumption of t-1 failure, shutting down...");
          System.exit(1);
        }
        // headStub updated & update healthStub
        // TODO: synchronization issue and locking
        Replicator.healthStub.get(shardId).remove(nodeId);
        Replicator.channels.get(shardId).remove(nodeId);

        Replicator.headStub[shardId] = RubbleKvStoreServiceGrpc.newStub(Replicator.channels.get(shardId).get(0));
        // Replicator.needRestart = observerAccumulator.longValue() + 1;
        Replicator.needRestart = maxThreads.longValue();
        System.out.println("[Restart times]: " + needRestart);

        // ping the node s.t. it will update the config
        pulse(true, true, shardId, 0);
        System.out.println("[Ops sent]: " + opssent.get());
      } else { // MIDDLE/TAIL NODE FAILURE
        LOGGER.error("middle node or tail node failure: shutdown...");
        System.exit(1);
      }
    }
    
    // recovery heartbeat
    private Empty pulse(boolean isAction, boolean isPrimary, 
                      int shardId, int nodeId) throws StatusRuntimeException{
      
      PingRequest request = PingRequest.newBuilder()
                                        .setIsAction(isAction)
                                        .setIsPrimary(isPrimary)
                                        .build();
      return Replicator.healthStub.get(shardId).get(nodeId)
                        .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                        .pulse(request);
    }
  }
}