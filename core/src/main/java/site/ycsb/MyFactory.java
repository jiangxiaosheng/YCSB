package site.ycsb;

import java.util.concurrent.*;
import java.io.*;
import java.net.*;

class MyFactory implements ThreadFactory {
  @Override
  public Thread newThread(Runnable r){
    Thread newt = new MyThread(r, "127.0.0.1", 1234);
    return newt;
  }
}

class MyThread extends Thread {
  private String dest;
  private int port;
  private Socket socket;
  private ObjectOutputStream out;
  private BufferedReader in;
  private Runnable r;

  public MyThread(Runnable r, String dest, int port) {
    this.dest = dest;
    this.port = port;
    this.r = r;

    // init the socket and open up the streams
    try {
      this.socket = new Socket(InetAddress.getByName(this.dest), this.port);
      this.out = new ObjectOutputStream(socket.getOutputStream());
      this.in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void run() {
    this.r.run();
  }

  public ObjectOutputStream getOutStream() {
    return this.out;
  }

  public BufferedReader getInStream() {
    return this.in;
  }

}
