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


public class Replicator {

  private ServerSocket servSock;

  public void start(int port) throws IOException {
    //setup db
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
        in = new BufferedReader(new InputStreamReader(instream));
        String inputLine;
        //while ((inputLine=in.readLine())!= null) {
        
        //while(instream.available() > 0) {
        final byte[] data = instream.readAllBytes();
        Map<String, ByteIterator> result = new HashMap<>();
        System.out.println(data.length);
        deserializeValues(data, null, result);
        System.out.println("hello" + result.size());
        for (Map.Entry<String, ByteIterator> entry : result.entrySet()) {
          System.out.println(entry.getKey() + ":" + entry.getValue().toString());
        }
        
        /*
        ObjectInputStream objIn = new ObjectInputStream(instream);
        Map<Object, Object> ret = new HashMap<>();
        try {
          ret = (Map) objIn.readObject();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
        for (Map.Entry<Object, Object> entry : ret.entrySet()) {
          System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        */
        //}
        in.close();
        //objIn.close();
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
