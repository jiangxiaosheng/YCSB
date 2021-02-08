import java.io.*;
import java.net.*;
import site.ycsb.*;

public class Server {

  public static void main(String[] args) throws IOException, DBException {
    Replicator replicator = new Replicator();
    try {
      replicator.init("primary");
      replicator.start(1234);
    } catch (DBException | IOException e) {
      e.printStackTrace();
    }
    replicator.stop();
  }
}
