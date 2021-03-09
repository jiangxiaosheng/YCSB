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
  private int seq;

  public BatchOp(Workload workload, DB db, int opcount, Object workloadstate,
              String op, CountDownLatch loopLatch, int seq) {
    this.workload = workload;
    this.db = db;
    this.opcount = opcount;
    this.workloadstate = workloadstate;
    this.doTransaction = op.equals("transaction") ? true : false;
    this.loopLatch = loopLatch;
    // ((MyThread)Thread.currentThread()).setSeq(seq);
    this.seq = seq;

  }

  public void run() {
    ((MyThread)Thread.currentThread()).setSeq(seq);
    Socket s = ((MyThread)Thread.currentThread()).getSocket();
    try {
      if (doTransaction) {
        for(int i = 0; i < opcount; i++) {
          ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
          BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
          status = workload.doTransaction(db, workloadstate, out, in);
        }
      } else {
        for(int i = 0; i < opcount; i++) {
          ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
          BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
          status = workload.doInsert(db, workloadstate, out, in);
          // System.out.println("status: " + status);
        }
      }
    } catch(IOException e) {
      e.printStackTrace();
    }

    loopLatch.countDown();
  }
}
