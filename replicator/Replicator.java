import site.ycsb.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.net.*;
import com.google.gson.*;

public class Replicator {

  private ServerSocket servSock;
  private static int seq; //global seq number for operations
  private static ConcurrentHashMap<Integer, Socket> waiting; //TODO: modify this to concurrentHashMap
  private Thread replyT;
  private Feedback replyRun;

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
  private static List<ExecutorService> shardClient;

  public void init(Map<String, Integer> shardHeads, int threads) throws DBException {
    this.recv = Executors.newCachedThreadPool();
    //this.executor = Executors.newFixedThreadPool(1000);

    // create thread pool executors for different shard heads
    this.shardClient = new ArrayList<>();
    shardHeads.forEach((dest, port) -> 
      this.shardClient.add(Executors.newFixedThreadPool(threads, new site.ycsb.MyFactory(dest, port)))
    );
    // synchronized(Replicator.waiting) {
    this.waiting = new ConcurrentHashMap<>();
    this.seq = 0;
    // }

  }

  public void start(int clientPort, int replyPort) throws IOException {
    //start the server socket
    try {
      this.servSock = new ServerSocket(clientPort, 1000);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // start the listening thread that send replies back to client
    this.replyRun = new Feedback(replyPort);
    this.replyT = new Thread(this.replyRun);
    this.replyT.start();

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
    shutdownService(recv);
    for(ExecutorService exe: shardClient) {
      shutdownService(exe);
    }
    try {
      this.replyRun.terminate();
      this.replyT.join();
      //close the socket
      servSock.close();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    //cleanup the db
  }

  public static void main(String[] args) {
    Map<String, Integer> shardHeads = new HashMap<>();
    //note that each shard head has to have a unique ip
    shardHeads.put("128.110.153.129", 2345);
    Replicator replicator = new Replicator();
    try {
      replicator.init(shardHeads, 200);
      replicator.start(1234, 9876);
    } catch (DBException | IOException e) {
      e.printStackTrace();
    }
    replicator.stop();
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
						str = "{" + str.split("\\{", 2)[1];
						// System.out.println("string is: " + str);
            try {	
						  ReplicatorOp op = gson.fromJson(str, ReplicatorOp.class);
              //TODO: some load-distribution algo here to distribute request to shard heads
              //current implementation default to the first executor in list

              // add sequence number and forward to shard head
              //TODO: waiting should be changed into concurrentHashMap
              synchronized(waiting) {
                Replicator.waiting.put(Replicator.seq, this.clientSock);
                op.setSeq(Replicator.seq++);
              }

              str = gson.toJson(op) + "\n";
              //TODO: think more about sync mechanism
              synchronized(Replicator.shardClient) {
                Replicator.shardClient.get(0).execute(new Forward(str));
              }
							// break;
            } catch (Exception e) {
              System.err.println("replicator deserialization failure");
              e.printStackTrace();
            }
          }
        }
        // in.close();
        // clientSock.close();
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
      Socket sock = ((MyThread)Thread.currentThread()).getSocket();
      try {
        ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
        out.writeObject(this.op);
      } catch(IOException e) {
        e.printStackTrace();
      }
    }
  }

  private class Feedback implements Runnable {
    private int replyPort;
    private ExecutorService replyExe;
    private boolean isAlive;

    public Feedback(int replyPort) {
      this.replyPort = replyPort;
      this.replyExe = Executors.newCachedThreadPool();
      this.isAlive = true;
    }

    public void run() {
      ServerSocket replySock;
      try {

        replySock = new ServerSocket(this.replyPort, 200);
        while(this.isAlive) {
          this.replyExe.execute(new ReplyHandler(replySock.accept()));
        }
        shutdownService(replyExe);
        replySock.close();

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void terminate() {
      this.isAlive = false;
    }
  }
	
	private class MySocket {
		private Socket socket;
		private BufferedReader in;
		public MySocket(Socket socket, BufferedReader in) {
			this.socket = socket;
			this.in = in;
		}
		public Socket getSocket() { return socket; }
		public BufferedReader getInStream() { return in; }
	}

  private class ReplyHandler implements Runnable {
    Socket sock;
    Gson gson;

    public ReplyHandler(Socket sock) {
      this.sock = sock;
      this.gson = new Gson();
    }

    public void run() {
      try {
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String str;
        while((str = in.readLine()) != null) {
          if (str.length() == 0) {
            System.out.println("end of stream");
          } else if (str.length() < 7) {
            System.out.println(str + " is not a valid operation");
          } else {
            //TODO: error handling
            str = "{"+ str.split("\\{", 2)[1];
            //de-serialize json string and handle operation
						// System.out.println("reply: " + str);
            Reply reply = gson.fromJson(str, Reply.class);
            // check if any clientSock match in waiting HashMap
            int seq = reply.getSeq();
            System.out.println("this end: reply status " + reply.getStatus());

            // retrieve clientsock and send back reply
            Socket clientSock = Replicator.waiting.remove(seq);
            if(clientSock == null) {
              System.err.println("seq: " + seq + " found with no matching client sock");
              return;
            }
            ObjectOutputStream out = new ObjectOutputStream(clientSock.getOutputStream());
            out.writeObject(str + "\n\n");

          }
        }
				in.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  } 
}
