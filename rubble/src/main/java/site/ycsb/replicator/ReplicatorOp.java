package site.ycsb.replicator;
import java.util.*;

/**
  class to send over tcp.
 */

public class ReplicatorOp {

  private String table;
  private String key;
  private byte[] values;
  private String operation;
  private int seq;

  public ReplicatorOp(String t, String k, byte[] v, String o) {
    table = t;
    key = k;
    values = v;
    operation = o;
  }

  public void setSeq(int s) {
    this.seq = s;
  }

  public int getSeq() {
    return seq;
  }

  public String getTable() {
    return table;
  }

  public String getKey() {
    return key;
  }

  public byte[] getValues() {
    return values;
  }

  public String getOp(){
    return operation;
  }
}
