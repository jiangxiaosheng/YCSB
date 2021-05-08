package site.ycsb.db.rubble;

import rubblejava.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.System;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.yaml.snakeyaml.Yaml;

public class Replicator {

  private final int port;
  private final Server replicatorServer;

    /* constructor */
  public Replicator(int port, String[][] shards, int batchSize, int chanNum) {
    this.port = port;
    this.replicatorServer = ServerBuilder.forPort(this.port)
                .executor(Executors.newFixedThreadPool(64))
                .addService(new ReplicatorService(shards, batchSize, chanNum))
                .build();
    System.out.println("Replicator is running on port : " + port);
  }

  public void start() throws IOException {
    this.replicatorServer.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down Replicator since JVM is shutting down");
        try {
            Replicator.this.stop();
        } catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
        System.err.println("*** Replicator shut down");
        }
    });
  }

  public void stop() throws InterruptedException {
    if (this.replicatorServer != null) {
        this.replicatorServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
  * Await termination on the main thread since the grpc library uses daemon threads.
  */
  private void blockUntilShutdown() throws InterruptedException {
    if (this.replicatorServer != null) {
      this.replicatorServer.awaitTermination();
    }
  }
    
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    System.out.println("Reading configuration file...");
    Yaml yaml = new Yaml();
    // System.out.println("current dir : " + System.getProperty("user.dir"));

    InputStream inputStream = new FileInputStream("./rubble/src/main/resources/config.yml");
    Map<String, Object> obj = yaml.load(inputStream);
    System.out.println("Finished");
    LinkedHashMap<String, Object> Params = (LinkedHashMap<String, Object>)obj.get("rubble_params");
    LinkedHashMap<String, List<String>> shardPorts = (LinkedHashMap<String, List<String>>)Params.get("shard_ports");
    int shardNum = (int)Params.get("shard_num");
    int replicaNum = (int)Params.get("replica_num");
    int batchSize = (int)Params.getOrDefault("batch_size", 1);
    int chanNum = (int) Params.getOrDefault("num_chan", 1);
    System.out.println("Shard number: "+shardNum);
    System.out.println("Replica number(chain length): "+replicaNum);
    System.out.println("Batch size: "+batchSize);
    System.out.println("Num of channel: "  + chanNum);

    String[][] shards = new String[shardNum][2];
    int idx = 0;
    for (String shardTag: shardPorts.keySet()) {
      System.out.println("Shard: "+shardTag);
      List<String> ports = shardPorts.get(shardTag);
      String headPort = ports.get(0);
      String tailPort = ports.get(ports.size()-1);
      System.out.println("Head: " + headPort);
      System.out.println("Tail: " + tailPort);
      shards[idx] = new String[]{headPort, tailPort};
      idx++;
    }
    Replicator replicator = new Replicator(50050, shards, batchSize, chanNum);
    replicator.start();
    replicator.blockUntilShutdown();
  }

  //TODO: find a better way than round-robin to send replies back to clients
  private static class ReplicatorService extends RubbleKvStoreServiceGrpc.RubbleKvStoreServiceImplBase {

    private static ConcurrentHashMap<StreamObserver<OpReply>, Long> ycsbObs;
    private static ConcurrentHashMap<Long, StreamObserver<OpReply>> opObserverMap;
    private final List<ManagedChannel> tailChan;
    private final List<ManagedChannel> headChan;

    private int shardNum;
    private int chanNum;
    private int batchSize;
    private static final Logger LOGGER = Logger.getLogger(ReplicatorService.class.getName());

    public ReplicatorService(String[][] shards, int batchSize, int chanNum) {   
      ycsbObs = new ConcurrentHashMap<>();
      opObserverMap = new ConcurrentHashMap<>();
      // create channels --> default to 1 channels
      this.tailChan = new ArrayList<>();
      this.headChan = new ArrayList<>();
      this.chanNum = chanNum;
      this.batchSize = batchSize;
      System.out.println("batchSize: "+this.batchSize);
      setupShards(shards, this.chanNum);
    }

    private void setupShards(String[][] shards, int chanNum) {
      this.shardNum = shards.length;
      System.out.println("Replicator sees " + this.shardNum + " shards");
      for(String[] s: shards){
        // TODO: add assert format here
        System.out.println("shard head: " + s[0] + " shard tail: " + s[1]);
        allocChannel(s[0], chanNum, this.headChan);
        allocChannel(s[1], chanNum, this.tailChan);
      }
      System.out.println("number of channels: " + this.headChan.size());
    }
    
    // helper function to pre-allocate channels to communicate with shard heads and tails
    private void allocChannel(String addr, int chanNum, List<ManagedChannel> chanList) {
      for(int i = 0; i < chanNum; i++) {
        ManagedChannel chan = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
        chanList.add(chan);
        // stubList.add(RubbleKvStoreServiceGrpc.newStub(chan));
      }
    }                                                                            

    @Override
    public StreamObserver<Op> doOp(final StreamObserver<OpReply> ob) { 
      final Long tid = Thread.currentThread().getId();
      // System.out.println("thread: " + tid + " in doOp");
      ycsbObs.put(ob, tid);
      // create tail client that will use the write-back-ycsb stream
      // note that we create one tail observer per thread per shard
      final ConcurrentHashMap<Integer, StreamObserver<Op>> tailObs = initTailOb(tid);
      final ConcurrentHashMap<Integer, StreamObserver<Op>> headObs = initHeadOb(tid);
      final ConcurrentHashMap<Long, StreamObserver<OpReply>> dup = this.opObserverMap;
      
      final int batchSize = this.batchSize;
      final int shardNum = this.shardNum;
     
      return new StreamObserver<Op>(){
        int opcount = 0;
        // builder to cache put and get requests to each shard
        HashMap<Integer, Op.Builder> putBuilders = new HashMap<>();
        HashMap<Integer, Op.Builder> getBuilders = new HashMap<>();
        boolean isInit = true;    
        long startTimeNanos;

        int batchCount = 0;

        private void init() {
          startTimeNanos = System.nanoTime();
          // init a build per opType for each shard
          for (int i = 0; i < shardNum; i++) {
            putBuilders.put(i, Op.newBuilder());
            getBuilders.put(i, Op.newBuilder());
          }
          isInit = false;
        }

        @Override
        public void onNext(Op op) {
          // init the builders
          if(isInit) {
            init();
          }
          // ignore if empty op
          assert op.getOpsCount() > 0 : "empty op received";
          opcount += op.getOpsCount();
          Long id = op.getOps(0).getId();
          
          for(SingleOp sop: op.getOpsList()){
            byte[] by = sop.getKey().getBytes();
            int shardIdx = by[by.length -1]%shardNum;

            // System.out.println("Op : " + sop.getId());
            opObserverMap.put(sop.getId(), ob);
            if (sop.getType() == SingleOp.OpType.GET){
              Op.Builder builder = getBuilders.get(shardIdx);
              builder.addOps(sop);

              if(builder.getOpsCount() == batchSize ){
                tailObs.get(shardIdx).onNext(builder.build());
                builder.clear();
              }
            } else if(sop.getType() == SingleOp.OpType.PUT){ //PUT
              Op.Builder builder = putBuilders.get(shardIdx);
              builder.addOps(sop);

              if (builder.getOpsCount() == batchSize ){
                headObs.get(shardIdx).onNext(builder.build());
                builder.clear();
                // System.out.println("PUT batch to shard: " + shardIdx + " sent");
              }
            }
          }
        }

        @Override
        public void onError(Throwable t) {
            // System.err.println("ycsb observer failed: " + Status.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
            // send out all requests in cache 
          for (Map.Entry<Integer, Op.Builder> entry : putBuilders.entrySet()) {
            if (entry.getValue().getOpsCount() > 0) {
              // System.out.print("Thread: " + tid  + " Sent out " + opcount + " ops in total ");
              headObs.get(entry.getKey()).onNext(entry.getValue().build());
              putBuilders.get(entry.getKey()).clear();
            }
          }
          for (Map.Entry<Integer, Op.Builder> entry : getBuilders.entrySet()) {
            if (entry.getValue().getOpsCount() > 0) {
              tailObs.get(entry.getKey()).onNext(entry.getValue().build());
              putBuilders.get(entry.getKey()).clear();
            }
          }
          // System.out.println(" time: " + (System.nanoTime() - startTimeNanos ));
          // System.out.println(" ycsb incoming stream completed");
        }
      };  
    }

    @Override
    public StreamObserver<OpReply> sendReply(final StreamObserver<Reply> replyOb) {

      final ConcurrentHashMap<Long, StreamObserver<OpReply>> ycsb_tmps = this.opObserverMap;

      ConcurrentHashMap<StreamObserver<OpReply>, OpReply.Builder> replyBuilders = new ConcurrentHashMap<>();
      for(StreamObserver<OpReply> ob : ycsbObs.keySet()){
        replyBuilders.put(ob, OpReply.newBuilder());
      }
      // System.out.println("client " + Thread.currentThread().getId() + " initialize ycsb reply builders");

      return new StreamObserver<OpReply>(){
      int opReplyCount = 0;
      StreamObserver<OpReply> tmp;
        
      @Override
      public void onNext(OpReply opReply) {
        assert opReply.getRepliesCount() > 0;
        opReplyCount += opReply.getRepliesCount();

        // System.out.print("reply size : "  + opReply.getRepliesCount());
        // System.out.println(" ,Id range : [ " + opReply.getReplies(0).getId() + " , " + opReply.getReplies(opReply.getRepliesCount() - 1).getId() + " ]");
        for(SingleOpReply reply : opReply.getRepliesList()){
          // System.out.println("Reply Id : " + reply.getId());
          replyBuilders.get(opObserverMap.get(reply.getId())).addReplies(reply);
        }

        try{
          for(Map.Entry<StreamObserver<OpReply>, OpReply.Builder> entry : replyBuilders.entrySet()){
            OpReply.Builder replyBuilder = entry.getValue();
            if(replyBuilder.getRepliesCount() == batchSize){
              StreamObserver<OpReply> ycsbOb = entry.getKey();
              OpReply reply = replyBuilder.build();
              // System.out.println("Client " + Thread.currentThread().getId() + " Sent " + replyBuilder.getRepliesCount() + " To " + ycsbObs.get(ycsbOb));
              replyBuilder.clear();
              ycsbOb.onNext(reply);
            }
          }
          for(SingleOpReply reply : opReply.getRepliesList()){
            // System.out.println("Reply Id : " + reply.getId());
            opObserverMap.remove(reply.getId());
          }  

        } catch (Exception e) {
          System.out.println("Exception : " + e.getMessage());
          System.out.println("at opReplyCount: " + opReplyCount + " error connecting to ycsb tmp ob " + opReply.getReplies(0).getId());
          System.out.println("first key: " + opReply.getReplies(0).getKey());
        }
      }

        @Override
        public void onError(Throwable t) {
            // System.err.println("SendReply ob failed: " + Status.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
            System.out.println("sendReply ob completed");
            
        }
      };    
    }
    
    // add observer that could write to tail into Map<StreamObserver> obs
    private ConcurrentHashMap<Integer, StreamObserver<Op>> initTailOb(Long id) {
        // replies from tail node
      ConcurrentHashMap<Integer, StreamObserver<Op>> tailObs = new ConcurrentHashMap<>();
      StreamObserver<Op> tmp;
      for(int i = 0; i < this.shardNum; i++) {
        tmp = RubbleKvStoreServiceGrpc.newStub(this.tailChan.get(id.intValue()%this.chanNum + i*this.chanNum)).doOp(
            new StreamObserver<OpReply>(){
            @Override
            public void onNext(OpReply reply) {
              System.out.println("reply from tail ob");
            }

            @Override
            public void onError(Throwable t) {
                // System.err.println("tail observer failed: " + Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {
              System.out.println("tail node reply stream completed");
            }
        });
        tailObs.put(i, tmp);
      }
      return tailObs;
    }

    // add an observer to comm with head nodes into Map<long, StreamObserver<Op>> headObs
    private ConcurrentHashMap<Integer, StreamObserver<Op>> initHeadOb(Long id) {
      ConcurrentHashMap<Integer, StreamObserver<Op>> headObs = new ConcurrentHashMap<>();
      StreamObserver<Op> tmp;
      for(int i = 0; i < this.shardNum; i++) {
        tmp = RubbleKvStoreServiceGrpc.newStub(this.headChan.get(id.intValue()%this.chanNum + i*this.chanNum)).doOp(
        new StreamObserver<OpReply>(){
        @Override
        public void onNext(OpReply reply) {
            // do nothing on replies from primary  
        }

        @Override
        public void onError(Throwable t) {
            // System.err.println("head observer failed: " + Status.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
            System.out.println("head node reply stream completed");
        }
      });
      headObs.put(i, tmp);
      }
      return headObs;
    }       
  }
}
