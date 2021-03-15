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
}

