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

public class Replicator {

  private ServerSocket servSock;
  private RocksDB rocksDb;
  private String role;

  public void init(String role) throws DBException {
  
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
    private PrintWriter out;
    private BufferedReader in;
    private InputStream instream;


    public ClientHandler(Socket socket) {
      this.clientSock = socket;
    }

    public void run() {
      try {
        out = new PrintWriter(clientSock.getOutputStream(), true);
        instream = clientSock.getInputStream();

        int len;
        byte[] data = new byte[512];
        StringBuilder sb = new StringBuilder();

        while ((len = instream.read(data)) != -1) {
          sb.append(new String(data, 0, len));
        }
        String str = sb.toString().substring(7);
        System.out.println(str.length());
        System.out.println(str);

        Gson gson = new Gson();
        try {
          ReplicatorOp op = gson.fromJson(str, ReplicatorOp.class);
          System.out.println(op.getKey());
        } catch (Exception e) {
          e.printStackTrace();
        }

        instream.close();
        out.close();
        clientSock.close();
      } catch (IOException e) {
        e.printStackTrace();
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
}
