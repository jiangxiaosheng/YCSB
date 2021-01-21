import site.ycsb.*;

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
import com.google.gson.*;
import org.rocksdb.*;
import net.jcip.annotations.GuardedBy;

public class Replicator {

  private ServerSocket servSock;
  private String role;

  @GuardedBy("Replicator.class") private static RocksDB rocksDb = null;
  @GuardedBy("Replicator.class") private static int references = 0;
  @GuardedBy("Replicator.class") private static Path rocksDbDir = null;
  @GuardedBy("Replicator.class") private static Path optionsFile = null;
  @GuardedBy("Replicator.class") private static RocksObject dbOptions = null;

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  static final String PROPERTY_ROCKSDB_OPTIONS_FILE = "rocksdb.optionsfile";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  //private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

  public void init(String role) throws DBException {
    this.role = role;

    synchronized(Replicator.class) {
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

  public void start(int port) throws IOException {
    //start the server socket
    try {
      servSock = new ServerSocket(port);
    } catch (IOException e) {
      e.printStackTrace();
    }
    //handle requests
    while (true)
      new ClientHandler(servSock.accept()).start();
  }

  public void stop() {
    //close the socket
    try {
      servSock.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    //cleanup the db
  }

  private class ClientHandler extends Thread {
    private Socket clientSock;
    // private PrintWriter out;
    private BufferedReader in;
    private InputStream instream;

    public ClientHandler(Socket socket) {
      this.clientSock = socket;
    }

    public void run() {
      try {
        // out = new PrintWriter(clientSock.getOutputStream(), true);
        instream = clientSock.getInputStream();
        in = new BufferedReader(new InputStreamReader(instream));

        String str;

        while((str = in.readLine()) != null) {
          if (str.length() == 0) {
            System.out.println("end of stream");
          } else if (str.length() < 7) {
            System.out.println(str + " is not a valid operation");
          } else {
            str = "{"+ str.split("\\{", 2)[1];
            System.out.println(str);
            Gson gson = new Gson();
            //de-serialize json string and handle operation
            try {
              ReplicatorOp op = gson.fromJson(str, ReplicatorOp.class);
              opHandler(op);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
        in.close();
        instream.close();
        // out.close();
        clientSock.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void opHandler(ReplicatorOp op) {

    String table = op.getTable();
    String key = op.getKey();

    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      //System.out.println(op.getValues().length);
      if (op.getOp().equals("insert")) {
        rocksDb.put(cf, key.getBytes(UTF_8), op.getValues());
      }
      //return Status.OK;
      /* confirmation unit
      final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
      if(values == null) {
        System.out.println("value not writtein in db");
      }

      System.out.println(values);
      */

      System.out.println("status: ok");
    } catch(final RocksDBException e) {
      //LOGGER.error(e.getMessage(), e);
      //return Status.ERROR;
      System.out.println("status: error");
    }
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
   * Should only be called with a {@code synchronized(Replicator.class)` block}.
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
