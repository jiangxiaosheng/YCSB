package site.ycsb;

import java.util.concurrent.*;
import java.io.*;
import io.grpc.Channel;

/**
 * Custom thread with i/o streams and socket.
 */
public class MyThread extends Thread {
  private Channel channel;
  private Runnable r;
  private long opcount;
  private long avg;
  private long target;

  public MyThread(Runnable r, Channel channel, long target) {
    this.r = r;
    this.channel = channel;
    this.opcount = 0;
    this.avg = 0;
    this.target = target;
  }

  public void run() {
    this.r.run();
  }

  public Channel getChannel() {
    return this.channel;
  }
  
  public void updateAvg(long latency) {
    this.opcount++;
    this.avg = (this.opcount-1)*this.avg / this.opcount + latency/this.opcount;
    if (this.opcount == this.target) {
      this.printAvg();
    }
  }

  public void printAvg() {
    System.out.println("thread ID: " + this.getId() + " opcount: " + this.opcount + " avg latency " + this.avg);
  }
}

