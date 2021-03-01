package site.ycsb;

import java.util.concurrent.*;
import java.io.*;

/**
 * ThreadFactory to generate threads with i/o stream.
 */
public class MyFactory implements ThreadFactory {
  private String dest;
  private int port;

  public MyFactory(String dest, int port) {
    this.dest = dest;
    this.port = port;
  }

  @Override
  public Thread newThread(Runnable r){
    Thread newt = new MyThread(r, this.dest, this.port);
    return newt;
  }
}
