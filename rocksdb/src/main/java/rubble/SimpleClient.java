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

public class SimpleClient {

    private static final Logger logger = Logger.getLogger(SimpleClient.class.getName());
    private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub asyncStub;

    public SimpleClient(Channel channel) {
        asyncStub = RubbleKvStoreServiceGrpc.newStub(channel);
    }

    public CountDownLatch insert(String k, String v) {
        PutRequest request;
        final CountDownLatch finishLatch = new CountDownLatch(1);
        // StreamObserver<PutReply> replyObserver = new StreamObserver<PutReply>() {

        StreamObserver<PutRequest> requestObserver =
            asyncStub.put( new StreamObserver<PutReply>(){
                @Override
                public void onNext(PutReply reply) {
                    System.out.println("put reply: " + reply);
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
                request = PutRequest.newBuilder().setKey("key " + k).setValue("value "+v).build();
                requestObserver.onNext(request);
                System.out.println("req " + k + " sent");
                logger.info("end of insert");
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        // return the latch while receiving happens asynchronously
        return finishLatch;
    }

    public CountDownLatch read(String k) {
        GetRequest request;
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<GetRequest> requestObserver =
            asyncStub.get( new StreamObserver<GetReply>(){
                @Override
                public void onNext(GetReply reply) {
                    //System.out.println("get reply: " + reply);
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("get failed " + Status.fromThrowable(t));
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    //System.out.println("done with get reply");
                    finishLatch.countDown();
                }
            });

        try {
                request = GetRequest.newBuilder().setKey("key " + k).build();
                requestObserver.onNext(request);
                //System.out.println("get " + k + " sent");
                //logger.info("end of read");
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        // Mark the end of requests
        requestObserver.onCompleted();

        // return the latch while receiving happens asynchronously
        return finishLatch;
    }
    /* test driver for SimpleClient
    public static void main(String[] args) throws Exception{
        String user = "world";
        // Access a service running on the local machine on port 50050
        String target = "localhost:50050";
        // Allow passing in the user and target strings as command line arguments
        if (args.length > 0) {
            if ("--help".equals(args[0])) {
                System.err.println("Usage: [name [target]]");
                System.err.println("");
                System.err.println("  name    The name you wish to be greeted by. Defaults to " + user);
                System.err.println("  target  The server to connect to. Defaults to " + target);
                System.exit(1);
            }
            user = args[0];
        }
        if (args.length > 1) {
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
            SimpleClient client = new SimpleClient(channel);
            CountDownLatch finishLatch = client.insert();
            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                System.out.println("routeChat can not finish within 1 minutes");
            }
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
    */
    
}



