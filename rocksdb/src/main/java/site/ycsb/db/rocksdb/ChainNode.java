package site.ycsb.db.rocksdb;

// import site.ycsb.*;
// import site.ycsb.Status;
// import org.rocksdb.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import java.io.*;
// import java.nio.file.*;
// import java.util.*;

// import io.grpc.Server;
// import io.grpc.ServerBuilder;
// import io.grpc.stub.StreamObserver;
// import io.grpc.ManagedChannel;
// import io.grpc.ManagedChannelBuilder;
// import site.ycsb.RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub;

// import java.io.IOException;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.atomic.LongAccumulator;
// import java.util.concurrent.CountDownLatch;
// import java.util.concurrent.Executors;
// import java.util.concurrent.ExecutorService;

// import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * ChainNode's implementation in Rubble.
 *
 * @author Haoyu Li.
 */
public class ChainNode {
  // private static DB db;
  // private static Properties props;
  // private final int port;
  // private final Server server;
  // private static final Logger LOGGER = LoggerFactory.getLogger(ChainNode.class);
  // private final String table;
  // private final String nodeType;
  // private final ManagedChannel nextChannel;
  // private final RubbleKvStoreServiceStub nextStub;
  // private static final String HEAD = "head";
  // private static final String MID  = "mid";
  // private static final String TAIL = "tail";
  // private final LongAccumulator readOpsDone = new LongAccumulator(Long::sum, 0L);
  // private final LongAccumulator writeOpsDone = new LongAccumulator(Long::sum, 0L);
  // private final Thread statusThread;
  // private final CountDownLatch latch = new CountDownLatch(1);
  // private final int statusIntervalNS;
  // private final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(16);

  // public ChainNode() {
  //   this.port = Integer.parseInt(props.getProperty("port"));
  //   this.nodeType = props.getProperty("node.type");
  //   if (nodeType.equals(TAIL)) {
  //     this.nextChannel = null;
  //     this.nextStub = null;
  //   } else {
  //     String nextNode = props.getProperty("next.node");
  //     this.nextChannel = ManagedChannelBuilder.forTarget(nextNode).usePlaintext().build();
  //     this.nextStub = RubbleKvStoreServiceGrpc.newStub(this.nextChannel);
  //   }
  //   ServerBuilder serverBuilder = ServerBuilder.forPort(port).addService(new RubbleKvStoreService());
  //   this.server = serverBuilder.executor(threadPoolExecutor).build();
  //   this.table = props.getProperty("table", "usertable");
  //   String dbname = props.getProperty("db", "site.ycsb.BasicDB");
  //   statusIntervalNS = 1000000 * Integer.parseInt(props.getProperty("status.interval", "10"));

  //   try {
  //     ClassLoader classLoader = DBFactory.class.getClassLoader();
  //     Class dbclass = classLoader.loadClass(dbname);
  //     db = (DB) dbclass.newInstance();
  //     db.setProperties(props);
  //     db.init();
  //   } catch (Exception e) {
  //     e.printStackTrace();
  //   }
    
  //   statusThread = new Thread() {
  //     @Override
  //     public void run() {
  //       boolean alldone = false;
  //       while (!alldone) {
  //         long readOld = readOpsDone.longValue();
  //         long writeOld = writeOpsDone.longValue();
  //         try {
  //           alldone = latch.await(statusIntervalNS, TimeUnit.NANOSECONDS);
  //         } catch (InterruptedException e) {
  //           Thread.currentThread().interrupt();
  //           alldone = true;
  //         }
  //         double readRate = (readOpsDone.longValue() - readOld) * 1.0E9 / statusIntervalNS;
  //         double writeRate = (writeOpsDone.longValue() - writeOld) * 1.0E9 / statusIntervalNS;
  //         LOGGER.info("Overall {} ops/sec READ {} ops/sec Write {} ops/sec", 
  //             readRate + writeRate, readRate, writeRate);
  //       }
  //     }
  //   };
  // }

  // /** Start serving requests. */
  // public void start() throws IOException {
  //   server.start();
  //   statusThread.start();
  //   LOGGER.info("Server started, listening on " + port);
  //   Runtime.getRuntime().addShutdownHook(new Thread() {
  //     @Override
  //     public void run() {
  //       System.err.println("*** shutting down gRPC server since JVM is shutting down");
  //       try {
  //         ChainNode.this.stop();
  //       } catch (InterruptedException e) {
  //         e.printStackTrace(System.err);
  //       }
  //       System.err.println("*** server shut down");
  //     }
  //   });
  // }

  // public void stop() throws InterruptedException {
  //   statusThread.interrupt();
  //   try {
  //     statusThread.join();
  //   } catch (InterruptedException ignored) {
  //     // ignored
  //   }

  //   if (server != null) {
  //     threadPoolExecutor.shutdownNow();
  //     server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
  //   }
  //   if (nextChannel != null) {
  //     nextChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  //   }
  //   if (db != null) {
  //     try {
  //       db.cleanup();
  //     } catch (DBException e) {
  //       e.printStackTrace(System.err);
  //     }
  //   }
  // }

  // private void blockUntilShutdown() throws InterruptedException {
  //   if (server != null) {
  //     server.awaitTermination();
  //   }
  // }

  // public static void main(String[] args) throws Exception {
  //   props = Client.parseArguments(args);
  //   ChainNode server = new ChainNode();
  //   server.start();
  //   server.blockUntilShutdown();
  // }

  // private class RubbleKvStoreService extends RubbleKvStoreServiceGrpc.RubbleKvStoreServiceImplBase {
  //   RubbleKvStoreService() {}

  //   @Override
  //   public StreamObserver<Op> doOp(final StreamObserver<OpReply> responseObserver) {
  //     return new StreamObserver<Op>() {
  //       public Status process(Op request, int i) {
  //         OpType type = request.getOps(i).getType();
  //         String key = request.getOps(i).getKey();
  //         String value = null;
  //         Map<String, ByteIterator> values = new HashMap<>();
  //         switch (type) {
  //           case GET:
  //             return db.read(table, key, null, values);

  //           case PUT:
  //             value = request.getOps(i).getValue();
  //             RocksDBClient.deserializeValues(value.getBytes(UTF_8), null, values);
  //             return db.insert(table, key, values);

  //           case UPDATE:
  //             value = request.getOps(i).getValue();
  //             RocksDBClient.deserializeValues(value.getBytes(UTF_8), null, values);
  //             return db.update(table, key, values);

  //           default:
  //             System.err.println("Unsupported op type: " + type);
  //             break;
  //         }

  //         return Status.ERROR;
  //       }

  //       @Override
  //       public void onNext(Op request) {
  //         //LOGGER.info("receive request from previous node");
  //         boolean isWrite = request.getOps(0).getType() != OpType.GET;
  //         OpReply.Builder builder = OpReply.newBuilder();
  //         SingleOpReply.Builder replyBuilder = SingleOpReply.newBuilder();
  //         int batchSize = request.getOpsCount();

  //         for (int i = 0; i < batchSize; i++) {
  //           Status res = process(request, i);
  //           if (isWrite) {
  //             writeOpsDone.accumulate(1);
  //           } else {
  //             readOpsDone.accumulate(1);
  //           }
  //           if (!res.isOk() && !res.equals(Status.NOT_FOUND)) {
  //             LOGGER.error("Some request failed!");
  //             replyBuilder.setOk(false);
  //           } else {
  //             replyBuilder.setOk(true);
  //           }
  //           replyBuilder.setStatus(res.getName());
  //           replyBuilder.setType(request.getOps(i).getType());
  //           builder.addReplies(replyBuilder.build());
  //         }

  //         if (nodeType.equals(TAIL)) {
  //           //LOGGER.info("reply to replicator");
  //           builder.addTime(request.getTime(0));
  //           responseObserver.onNext(builder.build());
  //           responseObserver.onCompleted();
  //         } else {
  //           //LOGGER.info("forward to next node");
  //           StreamObserver<Op> requestObserver = nextStub.doOp(responseObserver);
  //           requestObserver.onNext(request);
  //           requestObserver.onCompleted();
  //         }
  //       }

  //       @Override
  //       public void onError(Throwable t) {
  //         LOGGER.error("Encountered error in read", t);
  //       }

  //       @Override
  //       public void onCompleted() {
  //       }
  //     };
  //   }
  // }
}