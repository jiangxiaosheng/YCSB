import site.ycsb.*;
import java.io.*;
import java.net.*;
import java.util.*;
import com.google.gson.*;
import org.rocksdb.*;
import net.jcip.annotations.GuardedBy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.nio.ByteBuffer;
import java.nio.file.*;

class Node {

  private ServerSocket s;
  private ExecutorService executor;
  private static int port;
  private static String dest;
  private static boolean isTail;

  @GuardedBy("Node.class") private static RocksDB rocksDb = null;
  @GuardedBy("Node.class") private static int references = 0;
  @GuardedBy("Node.class") private static Path rocksDbDir = null;
  @GuardedBy("Node.class") private static Path optionsFile = null;
  @GuardedBy("Node.class") private static RocksObject dbOptions = null;

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  static final String PROPERTY_ROCKSDB_OPTIONS_FILE = "rocksdb.optionsfile";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  // initialization
  public void init(boolean isTail, String dest, int port) throws DBException{
    this.isTail = isTail;
    this.dest = dest;
    this.port = port;

    synchronized(Node.class) {
      if(rocksDb == null) {
        rocksDbDir = Paths.get(PROPERTY_ROCKSDB_DIR);
        //LOGGER.info("RocksDB data dir: " + rocksDbDir);

        //String optionsFileString = PROPERTY_ROCKSDB_OPTIONS_FILE;
        String optionsFileString = null;
        if (optionsFileString != null) {
          optionsFile = Paths.get(optionsFileString);
          //LOGGER.info("RocksDB options file: " + optionsFile);
        }

        try {
          if (optionsFile != null) {
            rocksDb = initRocksDBWithOptionsFile();
          } else {
            rocksDb = initRocksDB();
          }
        } catch (final IOException | RocksDBException e) {
          throw new DBException(e);
        }
      }

      references++;
    }
  }

  public void start(int inPort) throws IOException{
    try {
      // set the max queueing size to 1000
      s = new ServerSocket(inPort, 1000);
      this.executor = Executors.newFixedThreadPool(500);
    } catch (IOException e) {
      e.printStackTrace();
    }
    // listening on upstream request
    while (true) {
      //TODO: modify this executor to have ObjectOutputStream of its own
      this.executor.execute(new UpstreamHandler(s.accept()));
    }
  }

  public void stop() {
    this.executor.shutdown();
    try {
      if (!this.executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
        this.executor.shutdownNow();
      } 
    } catch (InterruptedException e) {
      this.executor.shutdownNow();
    }
    //close the socket
    try {
      this.s.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    //cleanup the db
  }

  public static void main(String[] args){
    Node node = new Node();
    if (args.length < 4) {
			System.out.println("please run with inputs");
			System.out.println("  java <dependencies> Node isTail<true/false> dest_ip dest_port listen_port");
			return;
		}
    try {
      node.init(false, "127.0.0.1", 3456);
      node.start(1234);
      node.stop();
    } catch (DBException | IOException e) {
      e.printStackTrace();
    }
  }

  // the private class to handle requests from upstream
  private class UpstreamHandler implements Runnable {

    private Socket clientSock;
    private ObjectOutputStream out;
    private BufferedReader in;
    private Socket downstream;
    
    public UpstreamHandler(Socket socket) {
      this.clientSock = socket;
    }

    /*
     TODO here
     - handle request
     - if Status.OK: forward to next node
     - else: keep retry
    */
    public void run() {
      try {
        this.in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
        String str;
        while((str = in.readLine()) != null) {
          if (str.length() == 0) {
            System.out.println("end of stream");
          } else if (str.length() < 7) {
            System.out.println(str + " is not a valid operation");
          } else {
            //TODO: error handling
            str = "{"+ str.split("\\{", 2)[1];
            System.out.println(str);
            Gson gson = new Gson();
            //de-serialize json string and handle operation
            try {
              ReplicatorOp op = gson.fromJson(str, ReplicatorOp.class);
              // System.out.println("op received: " + op.getOp());
              Reply reply = new Reply();
              reply.setStatus(site.ycsb.Status.ERROR);
              // keep retry until Status.OK
              while (!reply.getStatus().isOk()) {
                reply = opHandler(op);
              }
              //TODO: this need to be in threadpool
              this.downstream = new Socket(InetAddress.getByName(Node.dest), Node.port);
              this.out = new ObjectOutputStream(downstream.getOutputStream());
              //forward the reply if isTail
              if (Node.isTail) {
                String json = gson.toJson(reply) + "\n";
                //System.out.println(json);
                out.writeObject(json);
              } else { //forward the orignal json
                out.writeObject(str);
              }
              //TODO: this will be in the threadpool
              this.out.close();
              this.downstream.close();
            } catch (Exception e) {
              System.out.println("seg: " + str);
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

  private Reply opHandler(ReplicatorOp op) {

    String table = op.getTable();
    String key = op.getKey();
    //byte[] ret = new byte[0];
    Reply reply = new Reply();
    reply.setOp(op.getOp());
    reply.setSeq(op.getSeq());

    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      //System.out.println(op.getValues().length);
      
      //process different ops
      switch (op.getOp()) {
        case "insert":
          rocksDb.put(cf, key.getBytes(UTF_8), op.getValues());
          //TODO: insert comm with two secondaries here
          reply.setStatus(site.ycsb.Status.OK);
          break;
        case "read":
          final byte[] val = rocksDb.get(cf, key.getBytes(UTF_8));
          if(val == null) {
            reply.setStatus(site.ycsb.Status.NOT_FOUND);
            System.out.println("read - status: not found");
          } else {
            reply.setValues(val);
            reply.setStatus(site.ycsb.Status.OK);
          }
          break;
        //TODO: need to expand the ReplicatorOp class fields
        //implementation of scan could be delayed -> dummy impl now
        case "scan":
          reply.setStatus(site.ycsb.Status.OK);
          break;
          /*
          try(final RocksIterator iterator = rocksDb.newIterator(cf)) {
            int iterations = 0;
            for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
                iterator.next()) {
              final HashMap<String, ByteIterator> values = new HashMap<>();
              deserializeValues(iterator.value(), fields, values);
              result.add(values);
              iterations++;
            }
          }*/
        case "update":
          final Map<String, ByteIterator> result = new HashMap<>();
          final Map<String, ByteIterator> update = new HashMap<>();
          final byte[] currentValues = rocksDb.get(cf, key.getBytes(UTF_8));
          if(currentValues == null) {
            reply.setStatus(site.ycsb.Status.NOT_FOUND);
            System.out.println("update - status: not found");
            break;
          }
          deserializeValues(currentValues, null, result);
          deserializeValues(op.getValues(), null, update);
          //update
          result.putAll(update);
          //store
          rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(result));
          reply.setStatus(site.ycsb.Status.OK);
          break;
        case "delete":
          rocksDb.delete(cf, key.getBytes(UTF_8));
          reply.setStatus(site.ycsb.Status.OK);
        default:
          break;
      }
      //System.out.println("status: ok");
    } catch(final IOException | RocksDBException e) {
      //LOGGER.error(e.getMessage(), e);
      reply.setStatus(site.ycsb.Status.ERROR);
      System.out.println("status: error");
      e.printStackTrace();
    }
    return reply;
  }


  private RocksDB initRocksDB() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    final List<String> cfNames = loadColumnFamilyNames();
    final List<ColumnFamilyOptions> cfOptionss = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

    for(final String cfName : cfNames) {
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
          .optimizeLevelStyleCompaction();
      final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
          cfName.getBytes(UTF_8),
          cfOptions
      );
      cfOptionss.add(cfOptions);
      cfDescriptors.add(cfDescriptor);
    }

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if(cfDescriptors.isEmpty()) {
      final Options options = new Options()
          .optimizeLevelStyleCompaction()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;
      return RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
    } else {
      final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final RocksDB db = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
      for(int i = 0; i < cfNames.size(); i++) {
        COLUMN_FAMILIES.put(cfNames.get(i), new ColumnFamily(cfHandles.get(i), cfOptionss.get(i)));
      }
      return db;
    }
  }

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(Node.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  private RocksDB initRocksDBWithOptionsFile() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    final DBOptions options = new DBOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    RocksDB.loadLibrary();
    OptionsUtil.loadOptionsFromFile(optionsFile.toAbsolutePath().toString(), Env.getDefault(), options, cfDescriptors);
    dbOptions = options;

    final RocksDB db = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);

    for(int i = 0; i < cfDescriptors.size(); i++) {
      String cfName = new String(cfDescriptors.get(i).getName());
      final ColumnFamilyHandle cfHandle = cfHandles.get(i);
      final ColumnFamilyOptions cfOptions = cfDescriptors.get(i).getOptions();

      COLUMN_FAMILIES.put(cfName, new ColumnFamily(cfHandle, cfOptions));
    }

    return db;
  }

  public void cleanup() throws DBException {
    //super.cleanup();
    synchronized (Replicator.class) {
      try {
        if (references == 1) {
          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getHandle().close();
          }

          rocksDb.close();
          rocksDb = null;

          dbOptions.close();
          dbOptions = null;

          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getOptions().close();
          }
          saveColumnFamilyNames();
          COLUMN_FAMILIES.clear();

          rocksDbDir = null;
        }

      } catch (final IOException e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private void createColumnFamily(final String name) throws RocksDBException {
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if(!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyOptions cfOptions;

        if (optionsFile != null) {
          // RocksDB requires all options files to include options for the "default" column family;
          // apply those options to this column family
          cfOptions = getDefaultColumnFamilyOptions(name);
        } else {
          cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();
        }

        final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
        );
        COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));
      }
    } finally {
      l.unlock();
    }
  }

  private void saveColumnFamilyNames() throws IOException {
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    try(final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file, UTF_8))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8));
      for(final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    if(Files.exists(file)) {
      try (final LineNumberReader reader =
               new LineNumberReader(Files.newBufferedReader(file, UTF_8))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }

  private ColumnFamilyOptions getDefaultColumnFamilyOptions(final String destinationCfName) {
    final ColumnFamilyOptions cfOptions;

    if (COLUMN_FAMILIES.containsKey("default")) {
      //LOGGER.warn("no column family options for \"" + destinationCfName + "\" " +
                  //"in options file - using options from \"default\"");
      cfOptions = COLUMN_FAMILIES.get("default").getOptions();
    } else {
      //LOGGER.warn("no column family options for either \"" + destinationCfName + "\" or " +
                  //"\"default\" in options file - initializing with empty configuration");
      cfOptions = new ColumnFamilyOptions();
    }
    //LOGGER.warn("Add a CFOptions section for \"" + destinationCfName + "\" to the options file, " +
                //"or subsequent runs on this DB will fail.");

    return cfOptions;
  }

  private static final class ColumnFamily {
    private final ColumnFamilyHandle handle;
    private final ColumnFamilyOptions options;

    private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
      this.handle = handle;
      this.options = options;
    }

    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public ColumnFamilyOptions getOptions() {
      return options;
    }
  }

}

