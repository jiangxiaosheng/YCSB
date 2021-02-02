package site.ycsb.db.rocksdb;

import java.util.*;
import site.ycsb.*;
/**
 class to send over tcp as a reply to client requests.
 */

public class Reply {

  private byte[] values;
  private String operation;
  private Status status;

  public Reply(byte[] v, String o, Status s) {
    values = v;
    operation = o;
    status = s;
  }

  public Reply() {}

  public void setValues(byte[] v) {
    values = v;
  }

  public void setOp(String o) {
    operation = o;
  }

  public void setStatus(Status s) {
    status = s;
  }

  public byte[] getValues() {
    return values;
  }

  public String getOp(){
    return operation;
  }

  public Status getStatus() {
    return status;
  }
}

