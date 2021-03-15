package site.ycsb;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.io.*;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * ThreadFactory to generate threads with i/o stream.
 */
public class MyFactory implements ThreadFactory {
  private String target;
  private List<ManagedChannel> channels;
  private int chan_idx;
  private int counter;

  public MyFactory(String target) {
    this.target = target;
    this.channels = new ArrayList<>();
    this.chan_idx = -1;
    this.counter = 0;
  }

  @Override
  public Thread newThread(Runnable r){
    // create a new managedchannel per 100 threads
    if (counter %50 == 0) {
        channels.add(ManagedChannelBuilder.forTarget(this.target)
            .usePlaintext()
            .build());
        this.chan_idx++;
        System.out.println("built new chan no: " + this.chan_idx);
    }
    counter++;

    Thread newt = new MyThread(r, this.channels.get(this.chan_idx));
    return newt;
  }
}
