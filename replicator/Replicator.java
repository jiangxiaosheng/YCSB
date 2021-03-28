package rubblejava;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Replicator {
    
    private final int ycsb_port;
    private final Server ycsb_server;

    /* constructor */
    public Replicator(int ycsb_p, String head_addr, String tail_addr) {
        this.ycsb_port = ycsb_p;
        this.ycsb_server = ServerBuilder.forPort(ycsb_p)
                    .executor(Executors.newFixedThreadPool(32))
                    .addService(new ReplicatorService(head_addr, tail_addr))
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
        Replicator replicator = new Replicator(50050, "128.110.155.63:50051", "128.110.155.63:50052");
        replicator.start();
        replicator.blockUntilShutdown();
    }


    private static class ReplicatorService extends RubbleKvStoreServiceGrpc.RubbleKvStoreServiceImplBase {
        private static ConcurrentHashMap<Long, StreamObserver<Op>> tail_obs;
        private static ConcurrentHashMap<Long, StreamObserver<Op>> head_obs;
        private final List<RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub> tailStub;
        private final List<ManagedChannel> tailChan;
        private final List<RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub> headStub;
        private final List<ManagedChannel> headChan;
        
    
        public ReplicatorService(String head_addr, String tail_addr) {   
            tail_obs = new ConcurrentHashMap<>();
            System.out.println("replicator start service with head: " + head_addr + " tail: " + tail_addr);
            // create channels --> default to 16 channels
            this.tailChan = new ArrayList<>();
            this.tailStub = new ArrayList<>();
            this.headChan = new ArrayList<>();
            this.headStub = new ArrayList<>();
            allocChannel(tail_addr, 16, this.tailChan, this.tailStub);
            allocChannel(head_addr, 16, this.headChan, this.headStub);
            
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
            // this.ycsb_obs.put(Thread.currentThread().getId(), ob);
            final Long tid = Thread.currentThread().getId();
            // System.out.println("thread: " + tid + " in doOp");

            // create tail client that will use the write-back-ycsb stream
            // note that we create one tail observer per thread
            initTailOb(tid, ob);
            initHeadOb(tid);

            // create the ycsb-op processing logic with the tail client
            // System.out.println("tail_client: " + this.obs.containsKey(tid));
            final StreamObserver<Op> tail_client = this.tail_obs.get(tid);
            final StreamObserver<Op> head_client = this.head_obs.get(tid);
            return new StreamObserver<Op>(){
                int opcount = 0;
                // Set set = new HashSet();
                @Override
                public void onNext(Op op) {
                    // GET --> go to TAIL
                    if (op.getOpsCount() > 0 && op.getOps(0).getType().getNumber() == 0) {
                        // System.out.println("tid: " + tid + " Thread " + Thread.currentThread().getId() + " onNext -> tail");
                        tail_client.onNext(op);
                    } else { // other ops going to HEAD
                        head_client.onNext(op);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    // System.err.println("ycsb observer failed: " + Status.fromThrowable(t));
                }

                @Override
                public void onCompleted() {
                    // System.out.println("ycsb incoming stream completed");
                    // System.out.println(set);
                    tail_client.onCompleted();
                    head_client.onCompleted();
                }
            };  
        }
        
        // add observer that could write to tail into Map<StreamObserver> obs
        private void initTailOb(Long id, StreamObserver<OpReply> ob) {
            final StreamObserver<OpReply> ycsb_ob = ob;
            // replies from tail node
            this.tail_obs.put(id, this.tailStub.get(id.intValue()%16).doOp( new StreamObserver<OpReply>(){
                @Override
                public void onNext(OpReply reply) {
                    // System.out.println("reply: " + reply.getKey());
                    // forward to ycsb client
                    ycsb_ob.onNext(reply);
                }

                @Override
                public void onError(Throwable t) {
                    // System.err.println("tail observer failed: " + Status.fromThrowable(t));
                    ycsb_ob.onCompleted();
                }

                @Override
                public void onCompleted() {
                    System.out.println("tail node reply stream completed");
                    ycsb_ob.onCompleted();
                }
            }));
        }

        // add an observer to comm with head nodes into Map<long, StreamObserver<Op>> head_obs
        private void initHeadOb(Long id) {
            // replies from tail node
            this.head_obs.put(id, this.headStub.get(id.intValue()%16).doOp( new StreamObserver<OpReply>(){
                @Override
                public void onNext(OpReply reply) {
                    // do nothing on replies from primary  
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("head observer failed: " + Status.fromThrowable(t));
                }

                @Override
                public void onCompleted() {
                    System.out.println("head node reply stream completed");
                }
            }));
        }       
    }
        
}

       


    

    