import java.io.*;
import java.net.*;

public class Server {

  public static void main(String[] args) throws IOException {
    Replicator replicator = new Replicator();
    try {
      replicator.start(1234);
    } catch (IOException e) {
      e.printStackTrace();
    }
    replicator.stop();
  }
}
