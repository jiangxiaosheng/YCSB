import java.util.*;

/**
  class to send over tcp.
 */

public class ReplicatorOp {

  private String table;
  private String key;
  private byte[] values;
  private String operation;

  public ReplicatorOp(String t, String k, byte[] v, String o) {
    table = t;
    key = k;
    values = v;
    operation = o;
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

