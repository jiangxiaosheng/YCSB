package site.ycsb.db.rocksdb;

/**
 * ReplyListener to react on the messages.
 */

public class ReplyListener{
  private String op;

  public void bindOp(String operation) {
    op = operation;
  }

  public String getOp() {
    return op;
  }

  //@Override
  public void onMessage(Reply reply) {
    System.out.println(op + "heard!");
  }
}