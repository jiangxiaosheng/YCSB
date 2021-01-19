package site.ycsb.db.rocksdb;

import site.ycsb.*;
import java.util.*;

/**
  class to send over tcp.
 */

public class ReplicatorOp {

  private String table;
  private String key;
  private Map<String, StringByteIterator> values;
  private String operation;

  public ReplicatorOp(String t, String k, Map<String, StringByteIterator> v, String o) {
    //TODO: typechecking?
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

  public Map<String, StringByteIterator> getValues() {
    return values;
  }

  public String getOp(){
    return operation;
  }
}
