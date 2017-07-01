/*
 * Copyright 2017 Lightcopy
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

package com.github.lightcopy.history.model;

import java.util.HashMap;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.github.lightcopy.history.event.TaskMetrics;

/**
 * Spark task metrics with merge and update, keeps track of internal shuffle metrics and updated
 * blocks.
 * Created separate class to make it JSON/Mongo serializable with some extra functionality.
 */
public class Metrics extends AbstractCodec<Metrics> {
  // == Mongo fields ==
  public static final String FIELD_EXECUTOR_METRICS = "executorMetrics";
  public static final String FIELD_SHUFFLE_READ_METRICS = "shuffleReadMetrics";
  public static final String FIELD_SHUFFLE_WRITE_METRICS = "shuffleWriteMetrics";
  public static final String FIELD_INPUT_METRICS = "inputMetrics";
  public static final String FIELD_OUTPUT_METRICS = "outputMetrics";
  public static final String FIELD_RESULT_SIZE = "resultSize";
  public static final String FIELD_JVM_GC_TIME = "jvmGcTime";
  public static final String FIELD_RESULT_SERIALIZATION_TIME = "resultSerializationTime";
  public static final String FIELD_MEMORY_BYTES_SPILLED = "memoryBytesSpilled";
  public static final String FIELD_DISK_BYTES_SPILLED = "diskBytesSpilled";

  // == Map keys ==
  public static final String EXECUTOR_DESERIALIZE_TIME = "executorDeserializeTime";
  public static final String EXECUTOR_DESERIALIZE_CPU_TIME = "executorDeserializeCpuTime";
  public static final String EXECUTOR_RUN_TIME = "executorRunTime";
  public static final String EXECUTOR_CPU_TIME = "executorCpuTime";

  public static final String SHUFFLE_REMOTE_BLOCKS_FETCHED = "shuffleRemoteBlocksFetched";
  public static final String SHUFFLE_LOCAL_BLOCKS_FETCHED = "shuffleLocalBlocksFetched";
  public static final String SHUFFLE_FETCH_WAIT_TIME = "shuffleFetchWaitTime";
  public static final String SHUFFLE_REMOTE_BYTES_READ = "shuffleRemoteBytesRead";
  public static final String SHUFFLE_LOCAL_BYTES_READ = "shuffleLocalBytesRead";
  public static final String SHUFFLE_TOTAL_RECORDS_READ = "shuffleTotalRecordsRead";

  public static final String SHUFFLE_BYTES_WRITTEN = "shuffleBytesWritten";
  public static final String SHUFFLE_WRITE_TIME = "shuffleWriteTime";
  public static final String SHUFFLE_RECORDS_WRITTEN = "shuffleRecordsWritten";

  public static final String INPUT_BYTES_READ = "inputBytesRead";
  public static final String INPUT_RECORDS_READ = "inputRecordsRead";

  public static final String OUTPUT_BYTES_WRITTEN = "outputBytesWritten";
  public static final String OUTPUT_RECORDS_WRITTEN = "outputRecordsWritten";

  // metrics related to executor
  private HashMap<String, Long> executorMetrics;

  // Metrics related to shuffle read aggregated across all shuffle dependencies.
  // This is defined only if there are shuffle dependencies in this task.
  private HashMap<String, Long> shuffleReadMetrics;

  // Metrics related to shuffle write, defined only in shuffle map stages.
  private HashMap<String, Long> shuffleWriteMetrics;

  // Metrics related to reading data from a 'org.apache.spark.rdd.HadoopRDD' or from persisted
  // data, defined only in tasks with input.
  private HashMap<String, Long> inputMetrics;

  // Metrics related to writing data externally (e.g. to a distributed filesystem),
  // defined only in tasks with output.
  private HashMap<String, Long> outputMetrics;

  // The number of bytes this task transmitted back to the driver as the TaskResult.
  private long resultSize;
  // Amount of time the JVM spent in garbage collection while executing this task.
  private long jvmGcTime;
  // Amount of time spent serializing the task result.
  private long resultSerializationTime;
  // The number of in-memory bytes spilled by this task.
  private long memoryBytesSpilled;
  // The number of on-disk bytes spilled by this task.
  private long diskBytesSpilled;

  public Metrics() {
    this.resultSize = 0L;
    this.jvmGcTime = 0L;
    this.resultSerializationTime = 0L;
    this.memoryBytesSpilled = 0L;
    this.diskBytesSpilled = 0L;

    // default executor metrics
    this.executorMetrics = new HashMap<String, Long>();
    this.executorMetrics.put(EXECUTOR_DESERIALIZE_TIME, 0L);
    this.executorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, 0L);
    this.executorMetrics.put(EXECUTOR_RUN_TIME, 0L);
    this.executorMetrics.put(EXECUTOR_CPU_TIME, 0L);

    // default shuffle read metrics
    this.shuffleReadMetrics = new HashMap<String, Long>();
    this.shuffleReadMetrics.put(SHUFFLE_REMOTE_BLOCKS_FETCHED, 0L);
    this.shuffleReadMetrics.put(SHUFFLE_LOCAL_BLOCKS_FETCHED, 0L);
    this.shuffleReadMetrics.put(SHUFFLE_FETCH_WAIT_TIME, 0L);
    this.shuffleReadMetrics.put(SHUFFLE_REMOTE_BYTES_READ, 0L);
    this.shuffleReadMetrics.put(SHUFFLE_LOCAL_BYTES_READ, 0L);
    this.shuffleReadMetrics.put(SHUFFLE_TOTAL_RECORDS_READ, 0L);

    // default shuffle write metrics
    this.shuffleWriteMetrics = new HashMap<String, Long>();
    this.shuffleWriteMetrics.put(SHUFFLE_BYTES_WRITTEN, 0L);
    this.shuffleWriteMetrics.put(SHUFFLE_WRITE_TIME, 0L);
    this.shuffleWriteMetrics.put(SHUFFLE_RECORDS_WRITTEN, 0L);

    this.inputMetrics = new HashMap<String, Long>();
    this.inputMetrics.put(INPUT_BYTES_READ, 0L);
    this.inputMetrics.put(INPUT_RECORDS_READ, 0L);

    this.outputMetrics = new HashMap<String, Long>();
    this.outputMetrics.put(OUTPUT_BYTES_WRITTEN, 0L);
    this.outputMetrics.put(OUTPUT_RECORDS_WRITTEN, 0L);
  }

  // == Getters ==
  public long getResultSize() {
    return resultSize;
  }

  public long getJvmGcTime() {
    return jvmGcTime;
  }

  public long getResultSerializationTime() {
    return resultSerializationTime;
  }

  public long getMemoryBytesSpilled() {
    return memoryBytesSpilled;
  }

  public long getDiskBytesSpilled() {
    return diskBytesSpilled;
  }

  public HashMap<String, Long> getExecutorMetrics() {
    return executorMetrics;
  }

  public HashMap<String, Long> getShuffleReadMetrics() {
    return shuffleReadMetrics;
  }

  public HashMap<String, Long> getShuffleWriteMetrics() {
    return shuffleWriteMetrics;
  }

  public HashMap<String, Long> getInputMetrics() {
    return inputMetrics;
  }

  public HashMap<String, Long> getOutputMetrics() {
    return outputMetrics;
  }

  // == Setters ==
  public void setResultSize(long value) {
    this.resultSize = value;
  }

  public void setJvmGcTime(long value) {
    this.jvmGcTime = value;
  }

  public void setResultSerializationTime(long value) {
    this.resultSerializationTime = value;
  }

  public void setMemoryBytesSpilled(long value) {
    this.memoryBytesSpilled = value;
  }

  public void setDiskBytesSpilled(long value) {
    this.diskBytesSpilled = value;
  }

  public void setExecutorMetrics(HashMap<String, Long> map) {
    this.executorMetrics = map;
  }

  public void setShuffleReadMetrics(HashMap<String, Long> map) {
    this.shuffleReadMetrics = map;
  }

  public void setShuffleWriteMetrics(HashMap<String, Long> map) {
    this.shuffleWriteMetrics = map;
  }

  public void setInputMetrics(HashMap<String, Long> map) {
    this.inputMetrics = map;
  }

  public void setOutputMetrics(HashMap<String, Long> map) {
    this.outputMetrics = map;
  }

  /**
   * Set current metrics from `TaskMetrics` directly.
   * All values are updated to reflect task metrics.
   * @param taskMetrics task metrics
   */
  public void set(TaskMetrics taskMetrics) {
    setResultSize(taskMetrics.resultSize);
    setJvmGcTime(taskMetrics.jvmGcTime);
    setResultSerializationTime(taskMetrics.resultSerializationTime);
    setMemoryBytesSpilled(taskMetrics.memoryBytesSpilled);
    setDiskBytesSpilled(taskMetrics.diskBytesSpilled);

    executorMetrics.put(EXECUTOR_DESERIALIZE_TIME, taskMetrics.executorDeserializeTime);
    executorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, taskMetrics.executorDeserializeCpuTime);
    executorMetrics.put(EXECUTOR_RUN_TIME, taskMetrics.executorRunTime);
    executorMetrics.put(EXECUTOR_CPU_TIME, taskMetrics.executorCpuTime);

    shuffleReadMetrics.put(SHUFFLE_REMOTE_BLOCKS_FETCHED,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched);
    shuffleReadMetrics.put(SHUFFLE_LOCAL_BLOCKS_FETCHED,
      taskMetrics.shuffleReadMetrics.localBlocksFetched);
    shuffleReadMetrics.put(SHUFFLE_FETCH_WAIT_TIME,
      taskMetrics.shuffleReadMetrics.fetchWaitTime);
    shuffleReadMetrics.put(SHUFFLE_REMOTE_BYTES_READ,
      taskMetrics.shuffleReadMetrics.remoteBytesRead);
    shuffleReadMetrics.put(SHUFFLE_LOCAL_BYTES_READ,
      taskMetrics.shuffleReadMetrics.localBytesRead);
    shuffleReadMetrics.put(SHUFFLE_TOTAL_RECORDS_READ,
      taskMetrics.shuffleReadMetrics.totalRecordsRead);

    shuffleWriteMetrics.put(SHUFFLE_BYTES_WRITTEN,
      taskMetrics.shuffleWriteMetrics.shuffleBytesWritten);
    shuffleWriteMetrics.put(SHUFFLE_WRITE_TIME,
      taskMetrics.shuffleWriteMetrics.shuffleWriteTime);
    shuffleWriteMetrics.put(SHUFFLE_RECORDS_WRITTEN,
      taskMetrics.shuffleWriteMetrics.shuffleRecordsWritten);

    inputMetrics.put(INPUT_BYTES_READ, taskMetrics.inputMetrics.bytesRead);
    inputMetrics.put(INPUT_RECORDS_READ, taskMetrics.inputMetrics.recordsRead);

    outputMetrics.put(OUTPUT_BYTES_WRITTEN, taskMetrics.outputMetrics.bytesWritten);
    outputMetrics.put(OUTPUT_RECORDS_WRITTEN, taskMetrics.outputMetrics.recordsWritten);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Metrics)) return false;
    Metrics that = (Metrics) obj;
    return
      this.resultSize == that.resultSize &&
      this.jvmGcTime == that.jvmGcTime &&
      this.resultSerializationTime == that.resultSerializationTime &&
      this.memoryBytesSpilled == that.memoryBytesSpilled &&
      this.diskBytesSpilled == that.diskBytesSpilled &&
      this.executorMetrics.equals(that.executorMetrics) &&
      this.shuffleReadMetrics.equals(that.shuffleReadMetrics) &&
      this.shuffleWriteMetrics.equals(that.shuffleWriteMetrics) &&
      this.inputMetrics.equals(that.inputMetrics) &&
      this.outputMetrics.equals(that.outputMetrics);
  }

  // == Codec methods ==

  @Override
  public Metrics decode(BsonReader reader, DecoderContext decoderContext) {
    Metrics metrics = new Metrics();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_RESULT_SIZE:
          metrics.setResultSize(reader.readInt64());
          break;
        case FIELD_JVM_GC_TIME:
          metrics.setJvmGcTime(reader.readInt64());
          break;
        case FIELD_RESULT_SERIALIZATION_TIME:
          metrics.setResultSerializationTime(reader.readInt64());
          break;
        case FIELD_MEMORY_BYTES_SPILLED:
          metrics.setMemoryBytesSpilled(reader.readInt64());
          break;
        case FIELD_DISK_BYTES_SPILLED:
          metrics.setDiskBytesSpilled(reader.readInt64());
          break;
        case FIELD_EXECUTOR_METRICS:
          metrics.setExecutorMetrics(readMap(reader, LONG_ITEM));
          break;
        case FIELD_SHUFFLE_READ_METRICS:
          metrics.setShuffleReadMetrics(readMap(reader, LONG_ITEM));
          break;
        case FIELD_SHUFFLE_WRITE_METRICS:
          metrics.setShuffleWriteMetrics(readMap(reader, LONG_ITEM));
          break;
        case FIELD_INPUT_METRICS:
          metrics.setInputMetrics(readMap(reader, LONG_ITEM));
          break;
        case FIELD_OUTPUT_METRICS:
          metrics.setOutputMetrics(readMap(reader, LONG_ITEM));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return metrics;
  }

  @Override
  public Class<Metrics> getEncoderClass() {
    return Metrics.class;
  }

  @Override
  public void encode(BsonWriter writer, Metrics value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    writer.writeInt64(FIELD_RESULT_SIZE, value.getResultSize());
    writer.writeInt64(FIELD_JVM_GC_TIME, value.getJvmGcTime());
    writer.writeInt64(FIELD_RESULT_SERIALIZATION_TIME, value.getResultSerializationTime());
    writer.writeInt64(FIELD_MEMORY_BYTES_SPILLED, value.getMemoryBytesSpilled());
    writer.writeInt64(FIELD_DISK_BYTES_SPILLED, value.getDiskBytesSpilled());
    writeMap(writer, FIELD_EXECUTOR_METRICS, value.getExecutorMetrics(), LONG_ITEM);
    writeMap(writer, FIELD_SHUFFLE_READ_METRICS, value.getShuffleReadMetrics(), LONG_ITEM);
    writeMap(writer, FIELD_SHUFFLE_WRITE_METRICS, value.getShuffleWriteMetrics(), LONG_ITEM);
    writeMap(writer, FIELD_INPUT_METRICS, value.getInputMetrics(), LONG_ITEM);
    writeMap(writer, FIELD_OUTPUT_METRICS, value.getOutputMetrics(), LONG_ITEM);
    writer.writeEndDocument();
  }
}
