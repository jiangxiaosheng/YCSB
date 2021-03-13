package site.ycsb;

import java.util.concurrent.*;
import java.io.*;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Custom thread with i/o streams and socket.
 */
public class MyThread extends Thread {
  private Channel channel;
  private Runnable r;
  

  public MyThread(Runnable r, Channel channel) {
    this.r = r;
    this.channel = channel;
  }

  public void run() {
    this.r.run();
  }

  public Channel getChannel() {
    return this.channel;
  }

  /*
  public void setSeq(int s) {
    this.seq = s;
  }
  
  public int incSeq() {
    return this.seq++;
  }*/

}
