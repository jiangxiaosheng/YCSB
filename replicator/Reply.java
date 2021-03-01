import java.util.*;
import site.ycsb.*;
/**
  class to send over tcp as a reply to client requests
 */

public class Reply {

  private byte[] values;
  private String operation;
  private Status status;
  private int seq;

  public Reply(byte[] v, String o, Status s, int seq) {
    this.values = v;
    this.operation = o;
    this.status = s;
    this.seq = seq;
  }

  public Reply() {}

  public void setSeq(int s) {
    this.seq = s;
  }

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

  public int getSeq() {
    return seq;
  }
}

