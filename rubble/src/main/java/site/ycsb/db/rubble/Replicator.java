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
    System.out.println("current dir : " + System.getProperty("user.dir"));

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
    private static ConcurrentHashMap<Long, StreamObserver<Op>> tailObs;
    private static ConcurrentHashMap<Long, StreamObserver<Op>> headObs;
    private static ConcurrentHashMap<Long, StreamObserver<OpReply>> ycsbObs;
    private final List<ManagedChannel> tailChan;
    private final List<ManagedChannel> headChan;

    private int shardNum;
    private int chanNum;
    private int batchSize;
    private static final Logger LOGGER = Logger.getLogger(ReplicatorService.class.getName());

    public ReplicatorService(String[][] shards, int batchSize, int chanNum) {   
      tailObs = new ConcurrentHashMap<>();
      headObs = new ConcurrentHashMap<>();
      ycsbObs = new ConcurrentHashMap<>();
      // create channels --> default to 16 channels
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
        // add the write-back-to-ycsb stream to Map
            
      final Long tid = Thread.currentThread().getId();
      System.out.println("thread: " + tid + " in doOp");
        
      // create tail client that will use the write-back-ycsb stream
      // note that we create one tail observer per thread per shard
      final ConcurrentHashMap<Integer, StreamObserver<Op>> tailObs = initTailOb(tid);
      final ConcurrentHashMap<Integer, StreamObserver<Op>> headObs = initHeadOb(tid);
      final ConcurrentHashMap<Long, StreamObserver<OpReply>> dup = this.ycsbObs;
      
      final int batchSize = this.batchSize;
      final int shardNum = this.shardNum;
     
      return new StreamObserver<Op>(){
        int opcount = 0;
        // builder to cache put and get requests to each shard
        HashMap<Integer, Op.Builder> putBuilders = new HashMap<>();
        HashMap<Integer, Op.Builder> getBuilders = new HashMap<>();
        Op.Builder builder;
        boolean IsInit = true;    
        long startTimeNanos;
        int shardNum = 1;

        private void init(Long idx) {
          // LOGGER.info("Thread idx: " + tid + " init");
          startTimeNanos = System.nanoTime();
          if(dup.containsKey(idx)){
            System.out.println("duplicate key " + idx);
          }
          dup.put(idx, ob);
          assert shardNum > 0;
          for (int i = 0; i < shardNum; i++) {
            putBuilders.put(i, Op.newBuilder());
            System.out.println("put builder created and put to the map");
            getBuilders.put(i, Op.newBuilder());
          }
          builder = Op.newBuilder();
          System.out.println("builder initialized");
          IsInit = false;
        }

        @Override
        public void onNext(Op op) {
            
          // ignore if empty op
          assert op.getOpsCount() > 0:"empty op received";
          opcount += op.getOpsCount();
          Long idx = op.getOps(0).getId();
        
          // add the observer to map and check if overriding any other thread
          if(IsInit) {
            this.init(idx);
          }

          System.out.println("Received one op");

          for(SingleOp sop: op.getOpsList()){
            byte[] by = sop.getKey().getBytes();
            int shardIdx = by[by.length -1]%shardNum;
            // int shardIdx = 0;
            // int shardIdx = sop.getKey().getBytes()[10]%shardNum;
            // Long idxxx = sop.getId();
            // int shardIdx = (idxxx.intValue()) % shardNum;
            // System.out.println(shardIdx);
            if (sop.getType() == SingleOp.OpType.GET){
              Op.Builder builder = getBuilders.get(shardIdx);
              builder.addOps(sop);
              if(builder.getOpsCount() == batchSize ){
                // System.out.println("build one Get Op");
                tailObs.get(shardIdx).onNext(builder.build());

                // OpReply reply;
                // OpReply.Builder replyBuilder = OpReply.newBuilder();
                // for(SingleOp _sop: builder.getOpsList()){
                //     SingleOpReply singleReply = SingleOpReply.newBuilder().setKey(_sop.getKey()).build();
                //     replyBuilder.addReplies(singleReply);
                // }

                // reply = replyBuilder.build();
                // ob.onNext(reply);
                getBuilders.get(shardIdx).clear();

                // System.out.println("GET batch to shard: " + shardIdx + " sent");
              }
            } else { //PUT
            //   builder = putBuilders.get(shardIdx);
              builder.addOps(sop);
              if (builder.getOpsCount() == batchSize ){
                // System.out.println("build one Put Op");
                headObs.get(shardIdx).onNext(builder.build());
                // OpReply reply;
                // OpReply.Builder replyBuilder = OpReply.newBuilder();
                // for(SingleOp _sop: builder.getOpsList()){
                //     SingleOpReply singleReply = SingleOpReply.newBuilder().setKey(_sop.getKey()).setValue(_sop.getValue()).build();
                //     replyBuilder.addReplies(singleReply);
                // }
                // reply = replyBuilder.build();
                // ob.onNext(reply);
                // builder.clear();
                putBuilders.get(shardIdx).clear();
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
          System.out.println("Thread: " + tid + " time: " + (System.nanoTime() - startTimeNanos ));
          // System.out.println( " ycsb incoming stream completed");
        }
      };  
    }

    @Override
    public StreamObserver<OpReply> sendReply(final StreamObserver<Reply> ob) {
      final ConcurrentHashMap<Long, StreamObserver<OpReply>> ycsb_tmps = this.ycsbObs;
      // System.out.println("tmps: " + ycsb_tmps.mappingCount());
      return new StreamObserver<OpReply>(){
      int opCount = 0;
      StreamObserver<OpReply> tmp;
        
      @Override
      public void onNext(OpReply opReply) {
        assert opReply.getRepliesCount() >0;
        opCount += opReply.getRepliesCount();
        // System.out.println("opCount: " + opCount);
        if(opCount %10000 == 0) {
        // if (opCount == 250) {
        // System.out.println("opReply: " + opReply.getType() + " key" + opReply.getKey() + " id: " + opReply.getId() + " count: " + opCount);
          System.out.println("OpReply count" + opCount + " id: " + opReply.getReplies(0).getId());
        }
        // System.out.println("SendReply forward to ycsb, opCount: " + opCount);
        try {
        // need to add lock here to guarantee one write at a time
          tmp = ycsb_tmps.get(opReply.getReplies(0).getId());
            synchronized(tmp){
                tmp.onNext(opReply);
            }

        } catch (Exception e) {
          System.out.println("at opCount: " + opCount + " error connecting to ycsb tmp ob " + opReply.getReplies(0).getId());
          System.out.println("first key: " + opReply.getReplies(0).getKey());
        }
        // System.out.println("opCount: " + (++opCount));
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
      ConcurrentHashMap<Integer, StreamObserver<Op>> newMap = new ConcurrentHashMap<>();
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
        // newMap.put(BigInteger.valueOf(i), tmp);
        newMap.put(i, tmp);
        // System.out.println("added " + i + " to tail");
      }
      return newMap;
    }

    // add an observer to comm with head nodes into Map<long, StreamObserver<Op>> headObs
    private ConcurrentHashMap<Integer, StreamObserver<Op>> initHeadOb(Long id) {
      ConcurrentHashMap<Integer, StreamObserver<Op>> newMap = new ConcurrentHashMap<>();
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
        // newMap.put(BigInteger.valueOf(i), tmp);
      newMap.put(i, tmp);
        // System.out.println("added " + i + " to head");
      }
      return newMap;
    }       
  }
}
