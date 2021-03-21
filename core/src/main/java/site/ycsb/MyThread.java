package site.ycsb;

import java.util.concurrent.*;
import java.io.*;
import io.grpc.Channel;
import rubblejava.*;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Custom thread with i/o streams and socket.
 */
public class MyThread extends Thread {
  private Channel channel;
  private Runnable r;
  // private long opcount;
  private long sendCount;
  // private long avg;
  private long target;
  private StreamObserver<Op> observer;
  private final CountDownLatch latch;
  private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub asyncStub;

  public MyThread(Runnable r, Channel channel, long target) {
    this.r = r;
    this.channel = channel;
    // this.opcount = 0;
    // this.avg = 0;
    this.target = target;
    this.sendCount = 0;
    this.asyncStub = RubbleKvStoreServiceGrpc.newStub(this.channel);
    this.latch = this.createObserver();
  }

  public void run() {
    this.r.run();
  }

  public CountDownLatch createObserver() {
    final CountDownLatch finishLatch = new CountDownLatch(1);
    this.observer = asyncStub.doOp( new StreamObserver<OpReply>(){
      @Override
      public void onNext(OpReply reply) {
        OpReply r  = reply;
        // System.out.println("reply: " + reply.getKey() + "\n value: " + reply.getValue());
      }

      @Override
      public void onError(Throwable t) {
        System.err.println("get failed " + Status.fromThrowable(t));
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        System.out.println("done with reply");
        finishLatch.countDown();
      }
    });
    return finishLatch;
  }

  public void onNext(String k, String v, int seq, int op) {
    Op operation = Op.newBuilder().setKey(k).setValue(v)
                     .setId(seq).setType(Op.OpType.forNumber(op)).build();
    this.observer.onNext(operation);
    this.sendCount++;
    // System.out.println("sendCount: " + sendCount);
    if (this.sendCount == this.target) {
      this.observer.onCompleted();
      this.waitLatch();
    }
  }

  public void waitLatch(){
    try {
      this.latch.await();
      System.out.println("returned from latch for thread " + this.getId());
    } catch(InterruptedException e) {
      e.printStackTrace();
    }
  }

}

