package site.ycsb;

import java.util.concurrent.CountDownLatch;

/**
 * Runnable to execute single op in ClientThread.
 */


class SingleOp implements Runnable {
  private Workload workload;
  private DB db;
  private Object workloadstate;
  private boolean doTransaction;
  private boolean status;
  private final CountDownLatch loopLatch;

  public SingleOp(Workload workload, DB db, Object workloadstate, String op, CountDownLatch loopLatch) {
    this.workload = workload;
    this.db = db;
    this.workloadstate = workloadstate;
    this.doTransaction = op.equals("transaction") ? true : false;
    this.loopLatch = loopLatch;
  }

  public void run() {
    if (doTransaction) {
      status = workload.doTransaction(db, workloadstate);
    } else {
      status = workload.doInsert(db, workloadstate);
    }
    loopLatch.countDown();
  }
    //this.c.callback(); // callback
}