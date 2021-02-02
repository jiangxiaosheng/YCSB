package site.ycsb;

/**
 * Runnable to execute single op in ClientThread.
 */


class SingleOp implements Runnable {
  private Workload workload;
  private DB db;
  private Object workloadstate;
  private boolean doTransaction;

  public SingleOp(Workload workload, DB db, Object workloadstate, String op) {
    this.workload = workload;
    this.db = db;
    this.workloadstate = workloadstate;
    this.doTransaction = op.equals("transaction") ? true : false;
  }

  public void run() {
    if (doTransaction) {
      workload.doTransaction(db, workloadstate);
    } else {
      workload.doInsert(db, workloadstate);
    }
  }
    //this.c.callback(); // callback
}