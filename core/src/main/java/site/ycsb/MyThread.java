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
  // private Channel channel;
  private Runnable r;
  private long sendCount;
  private long target;
  private StreamObserver<Op> observer;
  private final CountDownLatch latch;
  private final RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub asyncStub;
  private final int batch_size;
  private Op.Builder op_builder;

  public MyThread(Runnable r, Channel channel, long target, int batch_size) {
    this.r = r;
    // this.channel = channel;
    this.target = target;
    this.sendCount = 0;
    this.asyncStub = RubbleKvStoreServiceGrpc.newStub(channel);
    this.batch_size = batch_size;
    this.latch = this.createObserver();
    this.op_builder = Op.newBuilder();
  }

  public void run() {
    this.r.run();
  }

  public CountDownLatch createObserver() {
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final long target = this.target;
    this.observer = asyncStub.doOp( new StreamObserver<OpReply>(){
      OpReply r = OpReply.newBuilder().build();
      int count;
      @Override
      public void onNext(OpReply reply) {
        r  = reply;
        count += r.getRepliesCount();
        //for(SingleOpReply sreply: reply.getRepliesList()) {
          //System.out.println("reply: " + sreply.getKey() + "status: " + sreply.getStatus());
        //}
	/*if (count % 10000 == 0) {
          System.out.println("reply: " + reply.getReplies(0).getKey() + "status: " + reply.getReplies(0).getStatus());
	}*/
      }

      @Override
      public void onError(Throwable t) {
        // System.err.println("get failed " + Status.fromThrowable(t));
        System.out.println("count: " + r.getRepliesCount());
        if (r.getRepliesCount() > 0) {
          SingleOpReply sreply = r.getReplies(0);
        // for(SingleOpReply sreply: r.getRepliesList()) {
          // System.out.println("reply: " + sreply.getKey() 
          //         + "status: " + sreply.getStatus()
          //         + "thread: " + Thread.currentThread().getId());
        }
          // System.out.println("thread processed counts: " + count + "target: " + target);
        // }
        assert count == target:"not valid";
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        for(SingleOpReply sreply: r.getRepliesList()) {
          // System.out.println("reply: " + sreply.getKey() + "status: " + sreply.getStatus());
        }
        // System.out.println("done with reply");
        assert count == target:"not valid";
        finishLatch.countDown();
      }
    });
    return finishLatch;
  }

  public void onNext(String k, String v, int seq, int op) {
    SingleOp operation = SingleOp.newBuilder().setKey(k).setValue(v)
                     .setId(seq).setType(SingleOp.OpType.forNumber(op)).build();
    this.op_builder.addOps(operation);
    this.sendCount++;
    if (this.sendCount > 0 && (this.sendCount % this.batch_size == 0 || this.sendCount == this.target)) {
      Op tmp = this.op_builder.build();
      // System.out.println("outgoing op size: " + tmp.getOpsCount() + " batch size: " + this.batch_size + "sendCount: " + this.sendCount);
      this.observer.onNext(tmp);
      this.op_builder = Op.newBuilder();
    }
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

