/**
 * <p>Rubble package client.</p>
 */

package rubblejava;

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

/**
 * Simple Client to call RocksDB through gRPC.
 */
public class SimpleClient {

    private static final Logger logger = Logger.getLogger(SimpleClient.class.getName());
    private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub asyncStub;
    private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceBlockingStub blockingStub; 

    public SimpleClient(Channel channel) {
        asyncStub = RubbleKvStoreServiceGrpc.newStub(channel);
        blockingStub = RubbleKvStoreServiceGrpc.newBlockingStub(channel);
    }


    public CountDownLatch doOp(String k, long seq, int operation) {
    // public CountDownLatch doOp(String k) {
        Op op;
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<Op> opObserver =
            asyncStub.doOp( new StreamObserver<OpReply>(){
                @Override
                public void onNext(OpReply reply) {
                    OpReply r  = reply;
                    System.out.println("get reply: " + reply);
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("get failed " + Status.fromThrowable(t));
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    finishLatch.countDown();
                    System.out.println("done with get reply");
                }
            });

        try {
                op = Op.newBuilder().setKey(k).setId(seq).setType(Op.OpType.forNumber(0)).build();
                System.out.println("request sent: " + op);
                opObserver.onNext(op);
                // Mark the end of requests
                //System.out.println("get " + k + " sent");
                //logger.info("end of read");
        } catch (RuntimeException e) {
            // Cancel RPC
            opObserver.onError(e);
            throw e;
        }

        opObserver.onCompleted();
        // return the latch while receiving happens asynchronously
        return finishLatch;
    }
    
}

