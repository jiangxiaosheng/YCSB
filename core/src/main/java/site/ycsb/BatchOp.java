package site.ycsb;

import java.util.concurrent.CountDownLatch;
import java.net.*;
import java.io.*;

/**
 * Runnable to execute single op in ClientThread.
 */

class BatchOp implements Runnable {
  private Workload workload;
  private DB db;
  private int opcount;
  private Object workloadstate;
  private boolean doTransaction;
  private boolean status;
  private final CountDownLatch loopLatch;
  private String dest;
  private int port;

  public BatchOp(Workload workload, DB db, int opcount, Object workloadstate,
              String op, CountDownLatch loopLatch) {
    this.workload = workload;
    this.db = db;
    this.opcount = opcount;
    this.workloadstate = workloadstate;
    this.doTransaction = op.equals("transaction") ? true : false;
    this.loopLatch = loopLatch;
    this.dest = "128.110.153.109";
    this.port = 1234;

  }

  public void run() {
    try {
      // long tk = System.nanoTime();
      Socket socket = new Socket(InetAddress.getByName(dest), port);
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 

      if (doTransaction) {
        for(int i = 0; i < opcount; i++) {
          status = workload.doTransaction(db, workloadstate);
        }
      } else {
        //long tk = System.nanoTime();
        for(int i = 0; i < opcount; i++) {
          status = workload.doInsert(db, workloadstate, out, in);
        }
        //System.out.println("interval: "+ (System.nanoTime() -tk));
      }

      in.close();
      out.close();
      socket.close();
      // System.out.println("time is " + (System.nanoTime() - tk));

    } catch (IOException e) {
      e.printStackTrace();
    }
    loopLatch.countDown();
  }
}
