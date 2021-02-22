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

  public BatchOp(Workload workload, DB db, int opcount, Object workloadstate,
              String op, CountDownLatch loopLatch) {
    this.workload = workload;
    this.db = db;
    this.opcount = opcount;
    this.workloadstate = workloadstate;
    this.doTransaction = op.equals("transaction") ? true : false;
    this.loopLatch = loopLatch;

  }

  public void run() {
    ObjectOutputStream out = ((MyThread)Thread.currentThread()).getOutStream();
    BufferedReader in = ((MyThread)Thread.currentThread()).getInStream();
    
    if (doTransaction) {
      for(int i = 0; i < opcount; i++) {
        status = workload.doTransaction(db, workloadstate, out, in);
      }
    } else {
      for(int i = 0; i < opcount; i++) {
        status = workload.doInsert(db, workloadstate, out, in);
      }
    }

    loopLatch.countDown();
  }
}
