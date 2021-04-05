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
  private Runnable r;
  private long sendCount;
  private long target;
  private StreamObserver<Op> observer;
  private final CountDownLatch latch;
  private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub asyncStub;
  private final int batch_size;
  private Op.Builder put_builder, get_builder;

  public MyThread(Runnable r, Channel channel, long target, int batch) {
    this.r = r;
    this.target = target;
    this.sendCount = 0;
    this.asyncStub = RubbleKvStoreServiceGrpc.newStub(channel);
    this.batch_size = batch;
    this.latch = this.createObserver();
    this.get_builder = Op.newBuilder();
    this.put_builder = Op.newBuilder();
  }

  public void run() {
    this.r.run();
  }

  public CountDownLatch createObserver() {
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final long t = this.target;
    this.observer = asyncStub.doOp( new StreamObserver<OpReply>(){
      long recvCount = 0;
      long tar = t;
      @Override
      public void onNext(OpReply reply) {
        OpReply r  = reply;
        // System.out.println("reply: " + reply.getReplies(0).getKey() /*+ "status: " + reply.getStatus()*/);
        // System.out.println("recvCount: " + recvCount + " tar: " + tar);
        recvCount += r.getRepliesCount();
        if(recvCount == tar) {
          System.out.println("recvCount: " + recvCount + " met target");
          finishLatch.countDown();
        }
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
    SingleOp operation = SingleOp.newBuilder().setKey(k).setValue(v)
                     .setId(Thread.currentThread().getId()).setType(SingleOp.OpType.forNumber(op)).build();
    // GET op
    if (op == 0) {
      this.get_builder.addOps(operation);
      if (this.get_builder.getOpsCount() == this.batch_size) {
        this.observer.onNext(this.get_builder.build());
        this.get_builder = Op.newBuilder();
      }
    } else { // PUT op
      this.put_builder.addOps(operation);
      if (this.put_builder.getOpsCount() == this.batch_size) {
        this.observer.onNext(this.put_builder.build());
        this.put_builder = Op.newBuilder();
        // System.out.println("this put builder cur size: " + this.put_builder.getOpsCount());
      }
    }

    this.sendCount++;
    if (this.sendCount == this.target) {
      // System.out.println("outgoing op size: " + tmp.getOpsCount() + " batch size: " + this.batch_size + "sendCount: " + this.sendCount);
      if (this.get_builder.getOpsCount() > 0) {
        System.out.println("tail get: " + this.get_builder.getOpsCount());
        this.observer.onNext(this.get_builder.build());
      }
      if (this.put_builder.getOpsCount() > 0) {
        System.out.println("tail put: " + this.put_builder.getOpsCount());
        this.observer.onNext(this.put_builder.build());
      }
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

