import site.ycsb.*;
import java.util.*;

/**
  class to send over tcp.
 */

public class ReplicatorOp {

  private String table;
  private String key;
  private Map<String, StringByteIterator> str_v = new HashMap<>();
  private Map<String, RandomByteIterator> rand_v = new HashMap<>();
  private String operation;
  private String dataType;

  public ReplicatorOp() {}

  public ReplicatorOp(String t, String k, Map<String, ByteIterator> v, String o, String type) {
    //TODO: typechecking?
    for (Map.Entry<String, ByteIterator> entry : v.entrySet()) {
      ByteIterator tmp = entry.getValue();
      if(tmp instanceof StringByteIterator){
        System.out.println("StringByteIterator");
        str_v.put(entry.getKey(), (StringByteIterator) entry.getValue());
      } else if (tmp instanceof ByteArrayByteIterator) {
        System.out.println("ByteArrayByteIterator");
      } else if (tmp instanceof InputStreamByteIterator) {
        System.out.println("InputStreamByteIterator");
      } else if (tmp instanceof RandomByteIterator) {
        System.out.println("RandomByteIterator");
        rand_v.put(entry.getKey(), (RandomByteIterator) entry.getValue());
      }else {
        System.out.println("not ByteIterator");
      }
    }
    table = t;
    key = k;
    operation = o;
  }

  public String getTable() {
    return table;
  }

  public String getKey() {
    return key;
  }

  public Map<String, ByteIterator> getValues() {
    Map<String, ByteIterator> map = new HashMap<>();
    for (Map.Entry<String, RandomByteIterator> entry : rand_v.entrySet()) {
      map.put(entry.getKey(), (ByteIterator) entry.getValue());
    }
    for (Map.Entry<String, StringByteIterator> entry : str_v.entrySet()) {
      map.put(entry.getKey(), (ByteIterator) entry.getValue());
    }
    return map;
  }

  public Map<String, StringByteIterator> getStringValues() {
    return str_v;
  }

  public String getOp(){
    return operation;
  }
}
