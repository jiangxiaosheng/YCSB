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

package site.ycsb.db.rubble;

import site.ycsb.*;
import site.ycsb.Status;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
// import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.ConcurrentMap;
// import java.util.concurrent.locks.Lock;
// import java.util.concurrent.locks.ReentrantLock;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
// import io.grpc.Status;
// import io.grpc.StatusRuntimeException;

import rubblejava.Op;
import rubblejava.SingleOp;
import rubblejava.OpReply;

import rubblejava.RubbleKvStoreServiceGrpc;
import rubblejava.RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 */
public class RubbleClient extends DB {

  private Properties props;
  // address of the replicator, rubble client should talk to the replicator
  private String targetAddr;
 
  private ManagedChannel chan;
  private RubbleKvStoreServiceStub asyncStub;
  // private RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub stub;
  private StreamObserver<Op> ob;

  private CountDownLatch latch;
  private Op.Builder opBuilder; 
  private int batchSize;
  // target opcount to perform
  private long currentOpCount;
  private long targetOpCount;

  private long batchCount;
  private static final Logger LOGGER = LoggerFactory.getLogger(RubbleClient.class);

  @GuardedBy("RubbleClient.class") private static int references = 0;

  @Override
  public void init() throws DBException{
    synchronized(RubbleClient.class) {
      if(this.chan == null) {
        this.props = this.getProperties();
        this.targetAddr = props.getProperty("targetaddr");
        // this.targetAddr = "0.0.0.0:50050";
        this.batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
        System.out.println("Target Address : " + targetAddr);
        System.out.println("Batch size : " + batchSize);

        this.currentOpCount = 0;
        final long threadOpCount = Integer.parseInt(props.getProperty("threadopcount"));
        System.out.println("Target op count : " + threadOpCount);
        this.targetOpCount = threadOpCount;

        this.chan = ManagedChannelBuilder.forTarget(targetAddr).usePlaintext().build();
        this.asyncStub = RubbleKvStoreServiceGrpc.newStub(this.chan);
        this.opBuilder = Op.newBuilder();

        final CountDownLatch finishLatch = new CountDownLatch(1);
        this.latch = finishLatch;
        this.ob = asyncStub.doOp(new StreamObserver<OpReply>(){
          long recvCount = 0;
          long target = threadOpCount;
          @Override
          public void onNext(OpReply reply) {
            recvCount += reply.getRepliesCount();
            if(recvCount == target) {
              System.out.println("recvCount: " + recvCount + " met target");
              finishLatch.countDown();
            }
          }

          @Override
          public void onError(Throwable t) {
            System.err.println("get failed " + io.grpc.Status.fromThrowable(t));
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            finishLatch.countDown();
          }
        });
        System.out.println("client initialized");
      }
      references++;
    }
  }

 
  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    synchronized (RubbleClient.class) {
      try {
        if (references == 1) {
          System.out.println("shut down client...");
          this.chan.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

      } catch (final Exception e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    try {
      // final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
      // if(values == null) {
      //   return Status.NOT_FOUND;
      // }
      // deserializeValues(values, fields, result);
      this.onNext(key, "", this.currentOpCount, SingleOp.OpType.GET_VALUE);
      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {
    try {
      // final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      // try(final RocksIterator iterator = rocksDb.newIterator(cf)) {
      //   int iterations = 0;
      //   for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
      //        iterator.next()) {
      //     final HashMap<String, ByteIterator> values = new HashMap<>();
      //     deserializeValues(iterator.value(), fields, values);
      //     result.add(values);
      //     iterations++;
      //   }
      // }
      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {

      // final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      final Map<String, ByteIterator> result = new HashMap<>();
      // final byte[] currentValues = rocksDb.get(cf, key.getBytes(UTF_8));
      // if(currentValues == null) {
      //   return Status.NOT_FOUND;
      // }
      // deserializeValues(currentValues, null, result);

      //update
      result.putAll(values);

      //store
      // rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(result));

      return Status.OK;

    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      this.onNext(key, new String(serializeValues(values)), this.currentOpCount, SingleOp.OpType.PUT_VALUE);
      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      // rocksDb.delete(cf, key.getBytes(UTF_8));
      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private void onNext(String k, String v, long id, int opType) {

    SingleOp op = SingleOp.newBuilder().setKey(k).setValue(v).
                          setId(id).setType(SingleOp.OpType.forNumber(opType)).
                          build();

    this.opBuilder.addOps(op);
    if (this.opBuilder.getOpsCount() == this.batchSize) {
      this.batchCount++;
      // System.out.println("sending " + this.batchCount + " th batch");
      this.ob.onNext(this.opBuilder.build());
      this.opBuilder.clear();
    }

    this.currentOpCount++;
    if (this.currentOpCount == this.targetOpCount) {
      if (this.opBuilder.getOpsCount() > 0) {
        this.ob.onNext(this.opBuilder.build());
      }
      this.ob.onCompleted();
      this.waitLatch();
    }
  }

  private void waitLatch(){
    try {
      // this.latch.await(1, TimeUnit.MINUTES);
      this.latch.wait();
      System.out.println("returned from latch for thread " + Thread.currentThread().getId());
    } catch(InterruptedException e) {
      e.printStackTrace();
    }
  }

  // private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
  //     final Map<String, ByteIterator> result) {
  //   final ByteBuffer buf = ByteBuffer.allocate(4);

  //   int offset = 0;
  //   while(offset < values.length) {
  //     buf.put(values, offset, 4);
  //     buf.flip();
  //     final int keyLen = buf.getInt();
  //     buf.clear();
  //     offset += 4;

  //     final String key = new String(values, offset, keyLen);
  //     offset += keyLen;

  //     buf.put(values, offset, 4);
  //     buf.flip();
  //     final int valueLen = buf.getInt();
  //     buf.clear();
  //     offset += 4;

  //     if(fields == null || fields.contains(key)) {
  //       result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
  //     }

  //     offset += valueLen;
  //   }

  //   return result;
  // }

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
