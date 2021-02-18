/*
 * Copyright (c) 2018 - 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.rocksdb;

import site.ycsb.*;
import site.ycsb.Status;
import net.jcip.annotations.GuardedBy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  static final String PROPERTY_ROCKSDB_OPTIONS_FILE = "rocksdb.optionsfile";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  @GuardedBy("RocksDBClient.class") private static Path rocksDbDir = null;
  @GuardedBy("RocksDBClient.class") private static Path optionsFile = null;
  @GuardedBy("RocksDBClient.class") private static RocksObject dbOptions = null;
  @GuardedBy("RocksDBClient.class") private static RocksDB rocksDb = null;
  @GuardedBy("RocksDBClient.class") private static int references = 0;
  private static long timer = 0;

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  @Override
  public void init() throws DBException {
    synchronized(RocksDBClient.class) {

      if(rocksDb == null) {
        rocksDbDir = Paths.get(getProperties().getProperty(PROPERTY_ROCKSDB_DIR));
        LOGGER.info("RocksDB data dir: " + rocksDbDir);

        String optionsFileString = getProperties().getProperty(PROPERTY_ROCKSDB_OPTIONS_FILE);
        if (optionsFileString != null) {
          optionsFile = Paths.get(optionsFileString);
          LOGGER.info("RocksDB options file: " + optionsFile);
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

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
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

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
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

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (RocksDBClient.class) {
      try {
        if (references == 1) {
          // System.out.println("timer: " + timer);
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


  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    Status ret = Status.ERROR;

    Socket socket;
    ObjectOutputStream out;
    BufferedReader in;

    ReplicatorOp op = new ReplicatorOp(table, key, null, new String("read"));
    Gson gson = new Gson();

    try {
      //add tcp socket for communication
      socket = new Socket(InetAddress.getByName("127.0.0.1"), 1234);
      out = new ObjectOutputStream(socket.getOutputStream());
      in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //add line break to read entries line by line
      String json = gson.toJson(op) + "\n";
      out.writeObject(json);
      // listeners.add(new GenericListener("read", result));
      ret = Status.OK;
      String str;

      while ((str = in.readLine()) != null) {
        if (str.length() == 0) {
          System.out.println("end of stream");
        } else if (str.length() < 7) {
          System.out.println(str + " is not a valid operation");
        } else {
          //TODO: error handling
          str = "{" + str.split("\\{", 2)[1];
          //de-serialize json string and handle operation
          Reply reply = gson.fromJson(str, Reply.class);
          deserializeValues(reply.getValues(), fields, result);
          break;
        }
      }
      in.close();
      out.close();
      socket.close();
      ret = Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
      // ret = Status.ERROR;
    } finally {
      return ret;
    }
  }


  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result, ObjectOutputStream out, BufferedReader in) {

    Status ret = Status.ERROR;
    ReplicatorOp op = new ReplicatorOp(table, key, null, new String("read"));
    Reply reply = iostream(op, out, in);
    ret = reply.getStatus();
    if (reply.getStatus().isOk()) {
      deserializeValues(reply.getValues(), fields, result);
    }
    return ret;
  }



  //dummy method
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {

    Socket socket;
    ObjectOutputStream out;
    BufferedReader in;

    ReplicatorOp op = new ReplicatorOp(table, startkey, null, new String("scan"));
    Gson gson = new Gson();
    Status ret = Status.ERROR;

    try {
      socket = new Socket(InetAddress.getByName("127.0.0.1"), 1234);
      out = new ObjectOutputStream(socket.getOutputStream());
      in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //add line break to read entries line by line
      String json = gson.toJson(op) + "\n";
      out.writeObject(json);

      /*
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      try(final RocksIterator iterator = rocksDb.newIterator(cf)) {
        int iterations = 0;
        for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
             iterator.next()) {
          final HashMap<String, ByteIterator> values = new HashMap<>();
          deserializeValues(iterator.value(), fields, values);
          result.add(values);
          iterations++;
        }
      }
      */
      ret = Status.OK;
      in.close();
      out.close();
      socket.close();
    } catch(final IOException e) {
      LOGGER.error(e.getMessage(), e);
      // ret = Status.ERROR;
    } finally {
      return ret;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result, ObjectOutputStream out, BufferedReader in) {

    Status ret = Status.ERROR;
    ReplicatorOp op = new ReplicatorOp(table, startkey, null, new String("scan"));
    //Reply reply = iostream(op, out, in);
    //ret = reply.getStatus();
    return ret;

  }


  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator
    //long cur = System.nanoTime();
    Socket socket;
    ObjectOutputStream out;
    BufferedReader in;

    Gson gson = new Gson();
    Status ret = Status.ERROR;

    try {

      socket = new Socket(InetAddress.getByName("127.0.0.1"), 1234);
      out = new ObjectOutputStream(socket.getOutputStream());
      in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //add line break to read entries line by line
      ReplicatorOp op = new ReplicatorOp(table, key, serializeValues(values), new String("update"));
      String json = gson.toJson(op) + "\n";
      out.writeObject(json);
      // listeners.add(new GenericListener("update", null));
      // timer += System.nanoTime() - cur;
      String str;
      while ((str = in.readLine()) != null) {
        if (str.length() == 0) {
          System.out.println("end of stream");
        } else if (str.length() < 7) {
          System.out.println(str + " is not a valid operation");
        } else {
          //TODO: error handling
          str = "{" + str.split("\\{", 2)[1];
          //de-serialize json string and handle operation
          Reply reply = gson.fromJson(str, Reply.class);
          ret = reply.getStatus();
          break;
        }
      }
      in.close();
      out.close();
      socket.close();
    } catch(final IOException e) {
      LOGGER.error(e.getMessage(), e);
      // ret = Status.ERROR;
    } finally {
      return ret;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values,
                        ObjectOutputStream out, BufferedReader in) {

    Status ret = Status.ERROR;
    try {
      ReplicatorOp op = new ReplicatorOp(table, key, serializeValues(values), new String("update"));
      Reply reply = iostream(op, out, in);
      ret = reply.getStatus();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      return ret;
    }
  }



  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    //long cur = System.nanoTime();
    Socket socket;
    ObjectOutputStream out;
    BufferedReader in;

    Gson gson = new Gson();
    Status ret = Status.ERROR;

    try {
      socket = new Socket(InetAddress.getByName("127.0.0.1"), 1234);
      out = new ObjectOutputStream(socket.getOutputStream());
      in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //add line break to read entries line by line
      ReplicatorOp op = new ReplicatorOp(table, key, serializeValues(values), new String("insert"));
      String json = gson.toJson(op) + "\n";
      out.writeObject(json);
      // listeners.add(new GenericListener("insert", null));
      //TODO: might need to do some confirmation before returning status.OK
      // timer += System.nanoTime() - cur;
      String str;
      while ((str = in.readLine()) != null) {
        if (str.length() == 0) {
          System.out.println("end of stream");
        } else if (str.length() < 7) {
          System.out.println(str + " is not a valid operation");
        } else {
          //TODO: error handling
          str = "{" + str.split("\\{", 2)[1];
          //de-serialize json string and handle operation
          Reply reply = gson.fromJson(str, Reply.class);
          ret = reply.getStatus();
          break;
        }
      }
      in.close();
      out.close();
      socket.close();
    } catch(final IOException e) {
      LOGGER.error(e.getMessage(), e);
      // ret = Status.ERROR;
    } finally {
      return ret;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values, 
                        ObjectOutputStream out, BufferedReader in) {
    Status ret = Status.ERROR;
    try {
      ReplicatorOp op = new ReplicatorOp(table, key, serializeValues(values), new String("insert"));
      Reply reply = iostream(op, out, in);
      ret = reply.getStatus();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      return ret;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    //long cur = System.nanoTime();
    Socket socket;
    ObjectOutputStream out;
    BufferedReader in;

    ReplicatorOp op = new ReplicatorOp(table, key, null, new String("delete"));
    Gson gson = new Gson();
    Status ret = Status.ERROR;

    try {
      socket = new Socket(InetAddress.getByName("127.0.0.1"), 1234);
      out = new ObjectOutputStream(socket.getOutputStream());
      in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //add line break to read entries line by line
      String json = gson.toJson(op) + "\n";
      out.writeObject(json);
      // listeners.add(new GenericListener("delete", null));
      // timer += System.nanoTime() - cur;
      String str;
      while ((str = in.readLine()) != null) {
        if (str.length() == 0) {
          System.out.println("end of stream");
        } else if (str.length() < 7) {
          System.out.println(str + " is not a valid operation");
        } else {
          //TODO: error handling
          str = "{" + str.split("\\{", 2)[1];
          //de-serialize json string and handle operation
          Reply reply = gson.fromJson(str, Reply.class);
          ret = reply.getStatus();
          break;
        }
      }
      in.close();
      out.close();
      socket.close();
    } catch(final IOException e) {
      LOGGER.error(e.getMessage(), e);
      ret = Status.ERROR;
    } finally {
      return ret;
    }
  }


  @Override
  public Status delete(final String table, final String key, ObjectOutputStream out, BufferedReader in) {
    Status ret = Status.ERROR;
    ReplicatorOp op = new ReplicatorOp(table, key, null, new String("delete"));
    Reply reply = iostream(op, out, in);
    ret = reply.getStatus();
    return ret;

  }


  private Reply iostream(ReplicatorOp op, ObjectOutputStream out, BufferedReader in) {

    Gson gson = new Gson();
    Status ret = Status.ERROR;
    Reply reply = new Reply(null, null, Status.ERROR);
    
    try {
      String json = gson.toJson(op) + "\n";
      out.writeObject(json);
      String str;
      while ((str = in.readLine()) != null) {
        if (str.length() == 0) {
          System.out.println("end of stream");
        } else if (str.length() < 7) {
          System.out.println(str + " is not a valid operation");
        } else {
          str = "{" + str.split("\\{", 2)[1];
          //de-serialize json string and handle operation
          reply = gson.fromJson(str, Reply.class);
          break;
        }
      }
    } catch(final IOException e) {
      LOGGER.error(e.getMessage(), e);
    } finally {
      return reply;
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

  private ColumnFamilyOptions getDefaultColumnFamilyOptions(final String destinationCfName) {
    final ColumnFamilyOptions cfOptions;

    if (COLUMN_FAMILIES.containsKey("default")) {
      LOGGER.warn("no column family options for \"" + destinationCfName + "\" " +
                  "in options file - using options from \"default\"");
      cfOptions = COLUMN_FAMILIES.get("default").getOptions();
    } else {
      LOGGER.warn("no column family options for either \"" + destinationCfName + "\" or " +
                  "\"default\" in options file - initializing with empty configuration");
      cfOptions = new ColumnFamilyOptions();
    }
    LOGGER.warn("Add a CFOptions section for \"" + destinationCfName + "\" to the options file, " +
                "or subsequent runs on this DB will fail.");

    return cfOptions;
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

  /*
  private class InParser implements Runnable {

    private volatile boolean dead = false;

    @Override
    public void run() {
      String str;
      Gson gson = new Gson();
      try {
        while(!dead) {
          while ((str = in.readLine()) != null) {
            if (str.length() == 0) {
              System.out.println("end of stream");
            } else if (str.length() < 7) {
              System.out.println(str + " is not a valid operation");
            } else {
              //TODO: error handling
              str = "{" + str.split("\\{", 2)[1];
              System.out.println("str is " + str);
              //de-serialize json string and handle operation
              Reply reply = gson.fromJson(str, Reply.class);
              synchronized (listeners) {
                for(ReplyListener l: listeners) {
                  l.onEvent(reply);
                  System.out.println("listener spoted");
                }
              }
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void stop() {
      dead = true;
    }
  }

  private class GenericListener implements ReplyListener {
    private Map<String, ByteIterator> result;
    private String op;
    private Status fill;

    public GenericListener(String op, final Map<String, ByteIterator> result, ) {
      this.op = op;
      this.result = result;
    }

    @Override
    public void onEvent(Reply reply) {
      System.out.println(op + " listener heard " + reply.getOp());
      //matching type of operation and listener
      /*
      if (reply.getOp().equals(op)) {
        switch (op){
          case "read":
            System.out.println("read");
            break;
          default:
            System.out.println();
            break;
          case "read":
            System.out.println("read");
            break;
          case "read":
            System.out.println("read");
            break;
          case "read":
            System.out.println("read");
            break;
        }
      }


    }

  }
  */

}
