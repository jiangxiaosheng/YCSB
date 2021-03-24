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
        Replicator replicator = new Replicator(50051, "", "128.110.153.114:50051");
        replicator.start();
        replicator.blockUntilShutdown();
    }


    private static class ReplicatorService extends RubbleKvStoreServiceGrpc.RubbleKvStoreServiceImplBase {
        private static ConcurrentHashMap<Long, StreamObserver<Op>> obs;
        private static ConcurrentHashMap<String, CountDownLatch> latches; 
        private static ConcurrentHashMap<String, Thread> listeners;
        private final List<RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub> tailStub;
        List<ManagedChannel> tailChan;
        // ManagedChannel head_chan;
        // private static Map<String, StreamObserver<OpReply>> reply_obs; ycsb_ob; // return OpReply
        // private final StreamObserver<Op> head_client; // return OpReply
        // private final StreamObserver<Op> tail_client; // return OpReply
        // private final CountDownLatch tail_latch;
        // private final CountDownLatch tail_create;


        public ReplicatorService(String head_addr, String tail_addr) {   
            obs = new ConcurrentHashMap<>();
            latches = new ConcurrentHashMap<>();
            listeners = new ConcurrentHashMap<>();
            // create channels
            tailChan = new ArrayList<>();
            tailStub = new ArrayList<>();
            for(int i = 0; i < 16; i++) {
                ManagedChannel chan = ManagedChannelBuilder.forTarget(tail_addr).usePlaintext().build();
                tailChan.add(chan);
                tailStub.add(RubbleKvStoreServiceGrpc.newStub(chan));
            }
            
        }

        @Override
        public StreamObserver<Op> doOp(final StreamObserver<OpReply> ob) {
            // TODO: think about multi-threading

            // add the write-back-to-ycsb stream to Map
            // this.ycsb_obs.put(Thread.currentThread().getId(), ob);
            final Long tid = Thread.currentThread().getId();
            // System.out.println("thread: " + tid + " in doOp");
            // create tail client that will use the write-back-ycsb stream
            initTailOb(tid, ob);

            final CountDownLatch ycsb_latch = new CountDownLatch(1);
            // this.latches.put("ycsb", ycsb_latch);
            // Thread t = new Thread(new Runnable() {
            //     @Override
            //     public void run() {
            //         try {
            //             ycsb_latch.await();
            //             // System.out.println("just finished");
            //             Thread.sleep(1000);
            //             // System.out.println("ycsb request completed");
            //             ob.onCompleted();
            //         } catch (InterruptedException e) {
            //             e.printStackTrace();
            //         }
            //     }
            // });
            // this.listeners.put("ycsb", t);
            // t.start();

            // create the ycsb-op processing logic with the tail client
            // System.out.println("tail_client: " + this.obs.containsKey(tid));
            final StreamObserver<Op> tail_client = this.obs.get(tid);
            return new StreamObserver<Op>(){
                int opcount = 0;
                // Set set = new HashSet();
                @Override
                public void onNext(Op op) {
                    // GET
                    if (op.getType().getNumber() == 0) {
                        // set.add(Thread.currentThread().getId());
                        // System.out.println("tid: " + tid + " Thread " + Thread.currentThread().getId() + " onNext -> tail");
                        // System.out.println("request key: " + op.getKey());
                        tail_client.onNext(op);
                    }

                }

                @Override
                public void onError(Throwable t) {
                    // System.err.println("ycsb observer failed: " + Status.fromThrowable(t));
                    ycsb_latch.countDown();
                }

                @Override
                public void onCompleted() {
                    // System.out.println("ycsb incoming stream completed");
                    // System.out.println(set);
                    tail_client.onCompleted();
                    ycsb_latch.countDown();
                }
            };  
        }

        private void initTailOb(Long id, StreamObserver<OpReply> ob) {
            // System.out.println("ycsb ob: " + this.obs.containsKey("ycsb"));
            final StreamObserver<OpReply> ycsb_ob = ob;
            // replies from tail node
            this.obs.put(id, this.tailStub.get(id.intValue()%16).doOp( new StreamObserver<OpReply>(){
                @Override
                public void onNext(OpReply reply) {
                    // System.out.println("reply: " + reply.getKey());
                    // forward to ycsb client
                    ycsb_ob.onNext(reply);
                    // Replicator.getObs("ycsb");
                }

                @Override
                public void onError(Throwable t) {
                    // System.err.println("tail observer failed: " + Status.fromThrowable(t));
                    ycsb_ob.onCompleted();
                    // tail_latch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("tail node reply stream completed");
                    ycsb_ob.onCompleted();
                    // tail_latch.countDown();
                }
            }));
            // return tail_latch;
        }       


    }

}

        // private CountDownLatch initHeadOb() {
        //     final CountDownLatch head_latch = new CountDownLatch(1);
        //     this.head_client = asyncStub.doOp( new StreamObserver<OpReply>(){
        //         @Override
        //         public void onNext(Op op) {
        //             // do nothing
        //         }

        //         @Override
        //         public void onError(Throwable t) {
        //             System.err.println("head observer failed: " + Status.fromThrowable(t));
        //             head_latch.countDown();
        //         }

        //         @Override
        //         public void onCompleted() {
        //             System.out.println("head node reply stream completed");
        //             head_latch.countDown();
        //         }
        //     });
        //     return head_latch;
        // }


    

    