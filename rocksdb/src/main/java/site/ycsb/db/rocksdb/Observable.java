package site.ycsb.db.rocksdb;

import site.ycsb.*;
import site.ycsb.Status;
import java.io.*;
import java.util.*;
import java.net.*;
import com.google.gson.*;


/**
 * Observable class to handle messages.
 */
public class Observable {

  private List<ReplyListener> observers;
  private BufferedReader instream;

  public Observable() {
    observers = new LinkedList<>();
  }

  public void addObserver(ReplyListener observer) {
    observers.add(observer);
  }

  public void addStream(BufferedReader in) {
    instream = in;
  }

  private  void notifyObservers(Reply reply) {
    String op = reply.getOp();
    System.out.println("notify: " + op);
    for (ReplyListener observer : observers) {
      if (op.equals(observer.getOp())) {
        observer.onMessage(reply);
      }
    }
  }

  public void onReply() {
    String str;
    Gson gson = new Gson();
    Reply reply = new Reply();
    try {
      System.out.println("hello onReply");
      while ((str = instream.readLine()) != null) {
        System.out.println(str);
        if (str.length() == 0) {
          System.out.println("end of stream");
        } else if (str.length() < 7) {//TODO: change this hardcoded version
          System.out.println(str + " is not a valid operation");
        } else {
          //TODO: error handling
          str = "{" + str.split("\\{", 2)[1];
          System.out.println("observable: " + str);
          //de-serialize json string and handle operation
          reply = gson.fromJson(str, Reply.class);
          break;
        }
      }
      System.out.println("break");
    } catch (IOException e) {
      //LOGGER.error(e.getMessage(), e);
      e.printStackTrace();
      reply.setStatus(Status.ERROR);
    }
    // Now we want to notify all the listeners about something.
    notifyObservers(reply);
  }
}