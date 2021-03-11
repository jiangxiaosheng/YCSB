/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.protobuf.Message;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
// import io.grpc.examples.routeguide.RouteGuideGrpc.RouteGuideBlockingStub;
// import io.grpc.examples.routeguide.RouteGuideGrpc.RouteGuideStub;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Demo of Node to Node forwarding - client part.
 */
public class Upstream {
  private static final Logger logger = Logger.getLogger(Upstream.class.getName());

  private final N2NGrpc.N2NBlockingStub blockingStub;
  private final N2NGrpc.N2NStub asyncStub;

  private Random random = new Random();

  /** Constructor. */
  public Upstream(Channel channel) {
    blockingStub = N2NGrpc.newBlockingStub(channel);
    asyncStub = N2NGrpc.newStub(channel);
  }

  /** Async unary rpc */
  public void forward(Request request) {
    // info("*** forward request: lat={0} lon={1}", lat, lon);
    info("*** forward request");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<Dull> responseObserver = new StreamObserver<Dull>() {
      @Override
      public void onNext(Dull dull) {
      }

      @Override
      public void onError(Throwable t) {
        warning("Forward Failed: {0}", Status.fromThrowable(t));
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        info("Finished Forwarding");
        finishLatch.countDown();
      }
    };
    for(int i = 0; i < 100; i++) {
      asyncStub.forward(request, responseObserver);
    }
  }

  /** Issues several different requests and then exits. */
  public static void main(String[] args) throws InterruptedException {
    String target = "localhost:8980";
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [target]");
        System.err.println("");
        System.err.println("  target  The server to connect to. Defaults to " + target);
        System.exit(1);
      }
      target = args[0];
    }

    Request request = Request.newBuilder()
                             .setTable("ycsb")
                             .setKey("hello")
                             .setOperation("read")
                             .setSeq(1).build();
    

    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    try {
      Upstream client = new Upstream(channel);
      // Looking for a valid feature
      client.forward(request);
      Thread.sleep(1000);
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private void info(String msg, Object... params) {
    logger.log(Level.INFO, msg, params);
  }

  private void warning(String msg, Object... params) {
    logger.log(Level.WARNING, msg, params);
  }
}

