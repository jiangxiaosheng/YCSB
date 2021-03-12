package rubble;

import com.google.protobuf.Message;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConcurrentClient {

    private static final Logger logger = Logger.getLogger(ConcurrentClient.class.getName());
    private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub asyncStub;
    private static CountDownLatch finish;

    public ConcurrentClient(Channel channel) {
        asyncStub = RubbleKvStoreServiceGrpc.newStub(channel);
    }

    public void insert(int start, CountDownLatch finishLatch) {
        PutRequest request;
        // final CountDownLatch finishLatch = new CountDownLatch(1);
        // StreamObserver<PutReply> replyObserver = new StreamObserver<PutReply>() {

        StreamObserver<PutRequest> requestObserver =
            asyncStub.put( new StreamObserver<PutReply>(){
                @Override
                public void onNext(PutReply reply) {
                    System.out.println("reply: " + reply);
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("put failed " + Status.fromThrowable(t));
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("done with put reply");
                    finishLatch.countDown();
                }
            });

        try {
            int num_of_kv = 3;
            for (int i = start; i < num_of_kv+start; i++) {
                request = PutRequest.newBuilder().setKey("key " + i).setValue("value "+i).build();
                requestObserver.onNext(request);
                System.out.println("req " + i + " sent");
            }
        
            logger.info("end of insert");
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        // return the latch while receiving happens asynchronously
        //return finishLatch;
    }


    public static void main(String[] args) throws Exception{
        // Access a service running on the local machine on port 50050
        String target = "localhost:50050";
        if (args.length > 0) {
            target = args[1];
        }

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .build();
        try {
            int num_of_threads = 100;
            finish = new CountDownLatch(num_of_threads); 
	    for(int i = 0; i< num_of_threads; i++) {
		new Thread(new Forward(i*3, channel, finish)).start();
	    }
            if (!finish.await(1, TimeUnit.MINUTES)) {
                System.out.println("concurrent clients can not finish within 1 minutes");
            }

        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static class Forward implements Runnable {
        private int seq;
        private Channel channel;
        private CountDownLatch latch;
        public Forward(int seq, Channel channel, CountDownLatch latch) {
            this.seq = seq;
            this.channel = channel;
            this.latch = latch;
        }
        @Override
        public void run() {
            ConcurrentClient client = new ConcurrentClient(this.channel);
            client.insert(this.seq, this.latch); 
        }
    }
    
}



