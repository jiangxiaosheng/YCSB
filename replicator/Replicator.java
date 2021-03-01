import site.ycsb.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.net.*;
import com.google.gson.*;

public class Replicator {

  private ServerSocket servSock;
  private ServerSocket replySock;

  /* 
   recv: responsible for receiving request from client and sending reply back
   shardClient: 
   - responsible for forwarding the request downstream
   - TODO: use customized threadfactory for these threadpool
   - TODO: write a threadpool that executes callable to take in responses from tail
      - listen on same port for responses from all three shards
      - once a match with sequence number, send the response back to client

  */
  private ExecutorService recv;
  private List<ExecutorService> shardClient;

  public void init(Map<String, Integer> shardHeads, int threads) throws DBException {
    this.recv = Executors.newCachedThreadPool();
    //this.executor = Executors.newFixedThreadPool(1000);

    // create thread pool executors for different shard heads
    this.shardClient = new ArrayList<>();
    shardHeads.forEach((dest, port) -> 
      this.shardClient.add(Executors.newFixedThreadPool(threads, new MyFactory(dest, port)))
    );
  }

  public void start(int clientPort, int replyPort) throws IOException {
    //start the server socket
    try {
      this.servSock = new ServerSocket(clientPort, 1000);
      this.replySock = new ServerSocket(replyPort, 1000);
    } catch (IOException e) {
      e.printStackTrace();
    }
    //handle requests
    while (true) {
      this.recv.execute(new ClientHandler(servSock.accept()));
    }
  }

  public void shutdownService(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      } 
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
  }
  public void stop() {
    for(ExecutorService exe: shardClient) {
      shutdownService(exe);
    }
    shutdownService(recv);
    //close the socket
    try {
      servSock.close();
      replySock.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    //cleanup the db
  }

  public static void main(String[] args) {
    Map<Integer, String> shardHeads = new HashMap<>();
    //note that each shard head has to have a unique ip
    shardHeads.put("127.0.0.1", 2345);

  }

  private class ClientHandler implements Runnable {
    private Socket clientSock;
    private ObjectOutputStream outstream;
    private BufferedReader in;

    public ClientHandler(Socket socket) {
      this.clientSock = socket;
    }

    public void run() {
      try {
        in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
        String str;
        while((str = in.readLine()) != null) {
          //System.out.println("str: " + str);
          if (str.length() == 0) {
            System.out.println("end of stream");
          } else if (str.length() < 7) {
            System.out.println(str + " is not a valid operation");
          } else {
            Gson gson = new Gson();
            //de-serialize json string and forward operations
            try {
              ReplicatorOp op = gson.fromJson(str, ReplicatorOp.class);
              //TODO: some load-distribution algo here to distribute request to shard heads
              //current implementation default to the first executor in list
              Replicator.shardClient.get(0).execute(new Forward(op));
              
            }catch (Exception e) {
              System.err.println("replicator deserialization failure")
              e.printStackTrace();
            }
          }
        }
        in.close();
        clientSock.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private class Forward implements Runnable {
    private String op;

    public Forward(String op) {
      this.op = op;
    }

    public void run() {
      ObjectOutputStream out = ((MyThread)Thread.currentThread()).getOutStream();
      out.writeObject(this.op);
    }
  }
  
}