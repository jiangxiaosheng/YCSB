/**
 * Copyright (c) 2010 Yahoo! Inc., 2016-2020 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import java.util.Map;

import site.ycsb.measurements.Measurements;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAccumulator;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.io.*;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import site.ycsb.RubbleKvStoreServiceGrpc.RubbleKvStoreServiceStub;

import java.util.concurrent.TimeUnit;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB {
  private final DB db;
  private final Measurements measurements;
  private final Tracer tracer;

  private boolean reportLatencyForEachError = false;
  private Set<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";

  private static final AtomicBoolean LOG_REPORT_CONFIG = new AtomicBoolean(false);

  private final String scopeStringCleanup;
  private final String scopeStringDelete;
  private final String scopeStringInit;
  private final String scopeStringInsert;
  private final String scopeStringRead;
  private final String scopeStringScan;
  private final String scopeStringUpdate;

  // [Rubble]
  private int shardNum;
  private int clientIdx;
  private StreamObserver<Op>[] requestObserver;
  private final LongAccumulator opsdone = new LongAccumulator(Long::sum, 0L);
  private final LongAccumulator opssent = new LongAccumulator(Long::sum, 0L);
  private static final LongAccumulator BATCH_ID = new LongAccumulator(Long::sum, 0L);
  private int opcount;
  private ManagedChannel channel;
  private RubbleKvStoreServiceStub asyncStub;
  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  // write batch
  private OpType[][] writeTypes;
  private String[][] writeKeys;
  private String[][] writeVals;
  private int[] writeBatchSize;
  // read batch
  private OpType[][] readTypes;
  private String[][] readKeys;
  private int[] readBatchSize;
  private boolean needReInit;
  // [Rubble]

  public DBWrapper(final DB db, final Tracer tracer) {
    this.db = db;
    measurements = Measurements.getMeasurements();
    this.tracer = tracer;
    final String simple = db.getClass().getSimpleName();
    scopeStringCleanup = simple + "#cleanup";
    scopeStringDelete = simple + "#delete";
    scopeStringInit = simple + "#init";
    scopeStringInsert = simple + "#insert";
    scopeStringRead = simple + "#read";
    scopeStringScan = simple + "#scan";
    scopeStringUpdate = simple + "#update";
  }

  // [Rubble]
  public int getOpsDone() {
    return opsdone.intValue();
  }

  public void setClientIdx(int clientidx) {
    this.clientIdx = clientidx;
  }

  public void setOpCount(int c) {
    this.opcount = c;
  }
  // [Rubble]

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    db.setProperties(p);
  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return db.getProperties();
  }

  private StreamObserver<OpReply> buildReplyObserver(int shardIdx) {
    return new StreamObserver<OpReply>() {
      private int shard = shardIdx;
      
      @Override
      public void onNext(OpReply reply) {
        //LOGGER.info("receive reply from replicator");
        int batchSize = reply.getRepliesCount();
        // long latency = (System.nanoTime() - reply.getTime(0)) / 1000;
        long latency = 0;
        for (int i = 0; i < batchSize; i++) {
          String suffix = "";
          if (!reply.getReplies(i).getOk()) {
            // LOGGER.error(reply.getReplies(i).getStatus());
            suffix = "-FAILED";
          }
          switch (reply.getReplies(i).getType()) {
            case GET:
              measurements.measure("READ" + suffix, (int)latency);
              break;
            case UPDATE:
              measurements.measure("UPDATE" + suffix, (int)latency);
              break;
            case PUT:
              measurements.measure("INSERT" + suffix, (int)latency);
              break;
            default:
              LOGGER.error("Unsupported type!");
              break;
          }
        }
        opsdone.accumulate(batchSize);
        if (opsdone.intValue() == opcount) {
          requestObserver[shard].onCompleted();
          System.out.println("requestObserver.onCompleted " + shard);
        }
      }

      @Override
      public void onError(Throwable t) {
        LOGGER.error("Encountered error in send", t);
      }

      @Override
      public void onCompleted() {
        if (opsdone.intValue() != opcount) {
          needReInit = true;
        }
        LOGGER.info("onCompleted from replicator");
        // LIMITER.release();
      }
    };
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringInit)) {
      db.init();

      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
          getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
              REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrorsProperty = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrorsProperty != null) {
          this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
              latencyTrackedErrorsProperty.split(",")));
        }
      }

      if (LOG_REPORT_CONFIG.compareAndSet(false, true)) {
        System.err.println("DBWrapper: report latency for each error is " +
            this.reportLatencyForEachError + " and specific error codes to track" +
            " for latency are: " + this.latencyTrackedErrors.toString());
      }
    }

    // [Rubble]
    channel = ManagedChannelBuilder.forTarget(getProperties().getProperty("replicator")).usePlaintext().build();
    asyncStub = RubbleKvStoreServiceGrpc.newStub(channel);
    needReInit = false;
    shardNum = Integer.parseInt(getProperties().getProperty("shard"));
    requestObserver = new StreamObserver[shardNum];
    for (int i = 0; i < shardNum; i++) {
      requestObserver[i] = asyncStub.doOp(buildReplyObserver(i));
    }
    writeTypes = new OpType[shardNum][DB.BATCHSIZE];
    writeKeys  = new String[shardNum][DB.BATCHSIZE];
    writeVals  = new String[shardNum][DB.BATCHSIZE];
    writeBatchSize = new int[shardNum];
    readTypes = new OpType[shardNum][DB.BATCHSIZE];
    readKeys  = new String[shardNum][DB.BATCHSIZE];
    readBatchSize = new int[shardNum];
    // [Rubble]
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringCleanup)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();

      // [Rubble]
      try {
        //LOGGER.info("shutdown the channel");
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOGGER.error("Failed to shutdown gRPC channle", e);
      }
      // [Rubble]

      db.cleanup();
      long en = System.nanoTime();
      measure("CLEANUP", Status.OK, ist, st, en);
    }
  }

  // [Rubble]
  public void sendBatch(boolean isWrite, int shard) throws Exception {
    int batchSize = isWrite ? writeBatchSize[shard] : readBatchSize[shard];
    assert(batchSize <= DB.BATCHSIZE);
    if (batchSize == 0) {
      return;
    }

    Op.Builder builder = Op.newBuilder();
    SingleOp.Builder opBuilder = SingleOp.newBuilder();
    builder.setHasEdits(false);
    builder.setShardIdx(shard);
    builder.setClientIdx(clientIdx);
    for (int i = 0; i < batchSize; i++) {
      opBuilder.setType(isWrite ? writeTypes[shard][i] : readTypes[shard][i]);
      opBuilder.setKey(isWrite ? writeKeys[shard][i] : readKeys[shard][i]);
      if (isWrite) {
        opBuilder.setValue(writeVals[shard][i]);
      }
      opBuilder.setTargetMemId(0);
      builder.addOps(opBuilder.build());
    }

    builder.addTime(System.nanoTime());
    BATCH_ID.accumulate(1);
    builder.setId(BATCH_ID.intValue());
    if (needReInit) {
      requestObserver[shard] = asyncStub.doOp(buildReplyObserver(shard));
      needReInit = false;
    }
    requestObserver[shard].onNext(builder.build());
    if (isWrite) {
      writeBatchSize[shard] = 0;
    } else {
      readBatchSize[shard] = 0;
    }
    opssent.accumulate(batchSize);
    if (opssent.intValue() == opcount) {
      for (int i = 0; i < shard; i++) {
        builder.setId(-1);
        builder.setShardIdx(i);
        requestObserver[i].onNext(builder.build());
        System.out.println("Client " + clientIdx + " sends TerminationMsg to shard " + i);
      }
    }
  }
  // [Rubble]

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try (final TraceScope span = tracer.newScope(scopeStringRead)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();
      // [Rubble]: send this op to replicator
      try {
        int idx = (int)(Long.parseLong(key.substring(4)) % shardNum);
        readTypes[idx][readBatchSize[idx]] = OpType.GET;
        readKeys[idx][readBatchSize[idx]] = key;
        readBatchSize[idx]++;
        if (readBatchSize[idx] == DB.BATCHSIZE) {
          sendBatch(false, idx);
        }
      } catch (Exception e) { 
        e.printStackTrace();
      }
      long en = System.nanoTime();
      Status res = Status.OK;
      // measure("READ", res, ist, st, en);
      measurements.reportStatus("READ", res);
      return res;
      // [Rubble]
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try (final TraceScope span = tracer.newScope(scopeStringScan)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();
      Status res = db.scan(table, startkey, recordcount, fields, result);
      long en = System.nanoTime();
      measure("SCAN", res, ist, st, en);
      measurements.reportStatus("SCAN", res);
      return res;
    }
  }

  private void measure(String op, Status result, long intendedStartTimeNanos,
                       long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (result == null || !result.isOk()) {
      if (this.reportLatencyForEachError ||
          this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    measurements.measure(measurementName,
        (int) ((endTimeNanos - startTimeNanos) / 1000));
    measurements.measureIntended(measurementName,
        (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try (final TraceScope span = tracer.newScope(scopeStringUpdate)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();
      // [Rubble]: send this op to replicator
      try {
        int idx = (int)(Long.parseLong(key.substring(4)) % shardNum);
        writeTypes[idx][writeBatchSize[idx]] = OpType.UPDATE;
        writeKeys[idx][writeBatchSize[idx]] = key;
        writeVals[idx][writeBatchSize[idx]] = new String(serializeValues(values));
        writeBatchSize[idx]++;
        if (writeBatchSize[idx] == DB.BATCHSIZE) {
          sendBatch(true, idx);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      long en = System.nanoTime();
      Status res = Status.OK;
      // measure("UPDATE", res, ist, st, en);
      measurements.reportStatus("UPDATE", res);
      return res;
      // [Rubble]
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try (final TraceScope span = tracer.newScope(scopeStringInsert)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();

      // [Rubble]: send this op to replicator
      try {
        int idx = (int)(Long.parseLong(key.substring(4)) % shardNum);
        writeTypes[idx][writeBatchSize[idx]] = OpType.PUT;
        writeKeys[idx][writeBatchSize[idx]] = key;
        writeVals[idx][writeBatchSize[idx]] = new String(serializeValues(values));
        writeBatchSize[idx]++;
        if (writeBatchSize[idx] == DB.BATCHSIZE) {
          sendBatch(true, idx);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      long en = System.nanoTime();
      Status res = Status.OK;
      // measure("INSERT", res, ist, st, en);
      measurements.reportStatus("INSERT", res);
      return res;
      // [Rubble]
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public Status delete(String table, String key) {
    try (final TraceScope span = tracer.newScope(scopeStringDelete)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();
      Status res = db.delete(table, key);
      long en = System.nanoTime();
      measure("DELETE", res, ist, st, en);
      measurements.reportStatus("DELETE", res);
      return res;
    }
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
