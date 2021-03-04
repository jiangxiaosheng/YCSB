package site.ycsb;

import java.util.concurrent.*;
import java.io.*;
import java.net.*;
/**
 * Custom thread with i/o streams and socket.
 */
public class MyThread extends Thread {
  private String dest;
  private int port;
  private Socket socket;
  private Runnable r;

  public MyThread(Runnable r, String dest, int port) {
    this.dest = dest;
    this.port = port;
    this.r = r;

    // init the socket and open up the streams
    try {
      this.socket = new Socket(InetAddress.getByName(this.dest), this.port);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void run() {
    this.r.run();
  }

  public Socket getSocket() {
    return this.socket;
  }

}
