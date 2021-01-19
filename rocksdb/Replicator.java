import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.net.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Replicator {

  private ServerSocket servSock;

  public void start(int port) throws IOException {
    try {
      servSock = new ServerSocket(port);
    } catch (IOException e) {
      e.printStackTrace();
    }
    while (true)
      new ClientHandler(servSock.accept()).start();
  }

  public void stop() {
    try {
      servSock.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static class ClientHandler extends Thread {
    private Socket clientSock;
    private PrintWriter out;
    private BufferedReader in;

    public ClientHandler(Socket socket) {
      this.clientSock = socket;
    }

    public void run() {
      try {
        out = new PrintWriter(clientSock.getOutputStream(), true);
        in = new BufferedReader(
          new InputStreamReader(clientSock.getInputStream()));

        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          if (".".equals(inputLine)) {
            out.println("bye");
            break;
          }
          System.out.println(inputLine);
        }

        in.close();
        out.close();
        clientSock.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
   }
  }
}
