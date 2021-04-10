package rubblejava;

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
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.math.BigInteger;
import org.yaml.snakeyaml.Yaml;


public class Replicator {
    
    private final int ycsb_port;
    private final Server ycsb_server;

    /* constructor */
    public Replicator(int ycsb_p, String[][] shards) {
        this.ycsb_port = ycsb_p;
        this.ycsb_server = ServerBuilder.forPort(ycsb_p)
                    .executor(Executors.newFixedThreadPool(64))
                    .addService(new ReplicatorService(shards))
                    .build();
    }

    public void start() throws IOException {
        this.ycsb_server.start();
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
        if (this.ycsb_server != null) {
            this.ycsb_server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
    * Await termination on the main thread since the grpc library uses daemon threads.
    */
    private void blockUntilShutdown() throws InterruptedException {
        if (this.ycsb_server != null) {
            this.ycsb_server.awaitTermination();
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.out.print("Reading configuration file...");
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream("config/test.yml");
        Map<String, Object> obj = yaml.load(inputStream);
        System.out.println("Finished");
        LinkedHashMap<String, Object> rubble_params = (LinkedHashMap<String, Object>)obj.get("rubble_params");
        LinkedHashMap<String, List<String>> shard_ports = (LinkedHashMap<String, List<String>>)rubble_params.get("shard_ports");
        int num_shards = (int)rubble_params.get("shard_num");
        int num_replica = (int)rubble_params.get("replica_num");
        System.out.println("Shard number: "+num_shards);
        System.out.println("Replica number(chain length): "+num_replica);

        String[][] shards = new String[num_shards][2];
        int shard_ind = 0;
        for (String shard_tag: shard_ports.keySet()) {
            System.out.println("Shard: "+shard_tag);
            List<String> ports = shard_ports.get(shard_tag);
            String head_port = ports.get(0);
            String tail_port = ports.get(ports.size()-1);
            System.out.println("Head: "+head_port);
            System.out.println("Tail: "+tail_port);
            shards[shard_ind] = new String[]{head_port, tail_port};
            shard_ind++;
        }
        Replicator replicator = new Replicator(50050, shards);
        replicator.start();
        replicator.blockUntilShutdown();
    }

    //TODO: find a better way than round-robin to send replies back to clients
    private static class ReplicatorService extends RubbleKvStoreServiceGrpc.RubbleKvStoreServiceImplBase {
        private static ConcurrentHashMap<Long, StreamObserver<Op>> tail_obs;
        private static ConcurrentHashMap<Long, StreamObserver<Op>> head_obs;
        private static ConcurrentHashMap<Long, StreamObserver<OpReply>> ycsb_obs;
        private final List<RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub> tailStub;
        private final List<ManagedChannel> tailChan;
        private final List<RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub> headStub;
        private final List<ManagedChannel> headChan;
        private int num_shards, num_channels;
    
        public ReplicatorService(String[][] shards) {   
            tail_obs = new ConcurrentHashMap<>();
            head_obs = new ConcurrentHashMap<>();
            ycsb_obs = new ConcurrentHashMap<>();
            // create channels --> default to 16 channels
            this.tailChan = new ArrayList<>();
            this.tailStub = new ArrayList<>();
            this.headChan = new ArrayList<>();
            this.headStub = new ArrayList<>();
            this.num_channels = 1;
            setupShards(shards, this.num_channels);
        }

        private void setupShards(String[][] shards, int num_chan) {
            this.num_shards = shards.length;
            System.out.println("Replicator sees " + this.num_shards + " shards");
            for(String[] s: shards){
                // TODO: add assert format here
                System.out.println("shard head: " + s[0] + " shard tail: " + s[1]);
                allocChannel(s[0], num_chan, this.headChan, this.headStub);
                allocChannel(s[1], num_chan, this.tailChan, this.tailStub);
            }
            System.out.println("number of channels: " + this.headChan.size());
        }
        
        // helper function to pre-allocate channels to communicate with shard heads and tails
        private void allocChannel(String addr, int num_chan, List<ManagedChannel> chanList,
                    List<RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub>stubList) {
            for(int i = 0; i < num_chan; i++) {
                ManagedChannel chan = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
                chanList.add(chan);
                stubList.add(RubbleKvStoreServiceGrpc.newStub(chan));
            }
        }                                                                            

        @Override
        public StreamObserver<Op> doOp(final StreamObserver<OpReply> ob) {
            // add the write-back-to-ycsb stream to Map
             
            final Long tid = Thread.currentThread().getId();
            System.out.println("thread: " + tid + " in doOp");

            // create tail client that will use the write-back-ycsb stream
            // note that we create one tail observer per thread per shard
            final ConcurrentHashMap<BigInteger, StreamObserver<Op>> tail_clients = initTailOb(tid);
            final ConcurrentHashMap<BigInteger, StreamObserver<Op>> head_clients = initHeadOb(tid);
            final ConcurrentHashMap<Long, StreamObserver<OpReply>> dup = this.ycsb_obs;
            //TODO: let cx know about this change
            final int batch_size = 10;
            final int mod_shard = this.num_shards;
            final BigInteger big_mod = BigInteger.valueOf(this.num_shards);

            return new StreamObserver<Op>(){
                int opcount = 0;
                // builder to cache put and get requests to each shard
                HashMap<BigInteger, Op.Builder> put_builder = new HashMap<>();
                HashMap<BigInteger, Op.Builder> get_builder = new HashMap<>();
                boolean hasInit = false;    
                Op.Builder builder_;            
                
                private void init(Long idx) {
                    if(dup.containsKey(idx)){
                        System.out.println("duplicate key " + idx);
                    }
                    dup.put(idx, ob);
                    for (int i = 0; i < mod_shard; i++) {
                        System.out.println("BigInteger value of: " + BigInteger.valueOf(i));
                        put_builder.put(BigInteger.valueOf(i), Op.newBuilder());
                        get_builder.put(BigInteger.valueOf(i), Op.newBuilder());
                    }
                    hasInit = true;
                }

                @Override
                public void onNext(Op op) {
                    
                    // ignore if empty op
                    assert op.getOpsCount() > 0:"empty op received";
                    opcount += op.getOpsCount();
                    Long idx = op.getOps(0).getId();
                    int mod = (idx.intValue())%mod_shard;
                    
                    // add the observer to map and check if overriding any other thread
                    if(!hasInit) {
                        this.init(idx);
                    }
                    // TODO: is there a better way to do sharding than converting to BigInteger
                    // i.e. bitmasking w/o converting to string or use stringbuilder
                    for(SingleOp sop: op.getOpsList()){
                        // BigInteger shard_idx = new BigInteger(sop.getKey().getBytes()).mod(big_mod);
                        BigInteger shard_idx = BigInteger.valueOf(0);
                        // System.out.println("shard idx: " + shard_idx);
                        if (sop.getType() == SingleOp.OpType.GET){
                            builder_ = get_builder.get(shard_idx);
                            builder_.addOps(sop);
                            if (builder_.getOpsCount() == batch_size ){
                                tail_clients.get(shard_idx).onNext(builder_.build());
                                get_builder.get(shard_idx).clear();
                                // System.out.println("GET batch to shard: " + shard_idx + " sent");
                            }
                        } else { //PUT
                            builder_ = put_builder.get(shard_idx);
                            builder_.addOps(sop);
                            if (builder_.getOpsCount() == batch_size ){
                                head_clients.get(shard_idx).onNext(builder_.build());
                                put_builder.get(shard_idx).clear();
                                // System.out.println("PUT batch to shard: " + shard_idx + " sent");
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
                    for (Map.Entry<BigInteger, Op.Builder> entry : put_builder.entrySet()) {
                        if (entry.getValue().getOpsCount() > 0) {
                            head_clients.get(entry.getKey()).onNext(entry.getValue().build());
                            put_builder.get(entry.getKey()).clear();
                        }
                    }
                    for (Map.Entry<BigInteger, Op.Builder> entry : get_builder.entrySet()) {
                        if (entry.getValue().getOpsCount() > 0) {
                            tail_clients.get(entry.getKey()).onNext(entry.getValue().build());
                            put_builder.get(entry.getKey()).clear();
                        }
                    }

                    System.out.println("ycsb incoming stream completed");
                    
                }
            };  
        }

        @Override
        public StreamObserver<OpReply> sendReply(final StreamObserver<Reply> ob) {
            final ConcurrentHashMap<Long, StreamObserver<OpReply>> ycsb_tmps = this.ycsb_obs;
            // System.out.println("tmps: " + ycsb_tmps.mappingCount());
            return new StreamObserver<OpReply>(){
                int opcount = 0;
                StreamObserver<OpReply> tmp;
                
                @Override
                public void onNext(OpReply op) {
                    assert op.getRepliesCount() >0;
                    opcount += op.getRepliesCount();
                    // System.out.println("opcount: " + opcount);
                    if(opcount % 100000 == 0) {
                    // if (opcount == 250) {
                        // System.out.println("op: " + op.getType() + " key" + op.getKey() + " id: " + op.getId() + " count: " + opcount);
                        System.out.println("OpReply count" + opcount + " id: " + op.getReplies(0).getId());
                    }
                    // System.out.println("SendReply forward to ycsb, ycsb is alive: " + !(ycsb_tmp == null));
                    try {
                        // need to add lock here to guarantee one write at a time
                        tmp = ycsb_tmps.get(op.getReplies(0).getId());
                        synchronized(tmp){
                            tmp.onNext(op);
                        }
                    } catch (Exception e) {
                        System.out.println("at opcount: " + opcount + " error connecting to ycsb tmp ob " + op.getReplies(0).getId());
                        System.out.println("first key: " + op.getReplies(0).getKey());
                    }
                    // System.out.println("opcount: " + (++opcount));
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
        private ConcurrentHashMap<BigInteger, StreamObserver<Op>> initTailOb(Long id) {
            // replies from tail node
            ConcurrentHashMap<BigInteger, StreamObserver<Op>> newMap = new ConcurrentHashMap<>();
            StreamObserver<Op> tmp;
            for(int i = 0; i < this.num_shards; i++) {
                tmp = this.tailStub.get(id.intValue()%this.num_channels+i*this.num_channels).doOp( new StreamObserver<OpReply>(){
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
                newMap.put(BigInteger.valueOf(i), tmp);
                // System.out.println("added " + i + " to tail");
            }
            return newMap;
        }

        // add an observer to comm with head nodes into Map<long, StreamObserver<Op>> head_obs
        private ConcurrentHashMap<BigInteger, StreamObserver<Op>> initHeadOb(Long id) {
            ConcurrentHashMap<BigInteger, StreamObserver<Op>> newMap = new ConcurrentHashMap<>();
            StreamObserver<Op> tmp;
            for(int i = 0; i < this.num_shards; i++) {
                tmp = this.headStub.get(id.intValue()%this.num_channels + i*this.num_channels).doOp( new StreamObserver<OpReply>(){
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
                newMap.put(BigInteger.valueOf(i), tmp);
                // System.out.println("added " + i + " to head");
            }
            return newMap;
        }       
    }
        
}

       


    

    