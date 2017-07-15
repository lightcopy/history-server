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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

import com.github.lightcopy.history.Mongo;

public class StageSummary extends AbstractCodec<StageSummary> {
  /** Class to store percentile values */
  public static class MetricPercentiles extends AbstractCodec<MetricPercentiles> {
    public static final String FIELD_MIN = "min";
    public static final String FIELD_PRC_25 = "prc25";
    public static final String FIELD_MEDIAN = "median";
    public static final String FIELD_PRC_75 = "prc75";
    public static final String FIELD_MAX = "max";

    long min;
    // 25th percentile
    long prc25;
    long median;
    // 75th percentile
    long prc75;
    long max;

    public MetricPercentiles() {
      this.min = 0L;
      this.prc25 = 0L;
      this.median = 0L;
      this.prc75 = 0L;
      this.max = 0L;
    }

    /** Find closest index for probability `p`, array must be sorted */
    private static int closestIndex(long[] data, double p) {
      int len = data.length;
      return Math.min((int) (p * len), len - 1);
    }

    public void setQuantiles(long[] data) {
      if (data != null && data.length > 0) {
        Arrays.sort(data);
        this.min = data[closestIndex(data, 0.0)];
        this.prc25 = data[closestIndex(data, 0.25)];
        this.median = data[closestIndex(data, 0.5)];
        this.prc75 = data[closestIndex(data, 0.75)];
        this.max = data[closestIndex(data, 1.0)];
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof MetricPercentiles)) return false;
      MetricPercentiles that = (MetricPercentiles) obj;
      return
        this.min == that.min &&
        this.prc25 == that.prc25 &&
        this.median == that.median &&
        this.prc75 == that.prc75 &&
        this.max == that.max;
    }

    /** Create metric percentiles from array */
    public static MetricPercentiles fromArray(long[] data) {
      MetricPercentiles metrics = new MetricPercentiles();
      metrics.setQuantiles(data);
      return metrics;
    }

    // == Codec methods ==

    @Override
    public MetricPercentiles decode(BsonReader reader, DecoderContext decoderContext) {
      MetricPercentiles metrics = new MetricPercentiles();
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        switch (reader.readName()) {
          case FIELD_MIN:
            metrics.min = reader.readInt64();
            break;
          case FIELD_PRC_25:
            metrics.prc25 = reader.readInt64();
            break;
          case FIELD_MEDIAN:
            metrics.median = reader.readInt64();
            break;
          case FIELD_PRC_75:
            metrics.prc75 = reader.readInt64();
            break;
          case FIELD_MAX:
            metrics.max = reader.readInt64();
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
    public Class<MetricPercentiles> getEncoderClass() {
      return MetricPercentiles.class;
    }

    @Override
    public void encode(BsonWriter writer, MetricPercentiles value, EncoderContext encoderContext) {
      writer.writeStartDocument();
      writer.writeInt64(FIELD_MIN, value.min);
      writer.writeInt64(FIELD_PRC_25, value.prc25);
      writer.writeInt64(FIELD_MEDIAN, value.median);
      writer.writeInt64(FIELD_PRC_75, value.prc75);
      writer.writeInt64(FIELD_MAX, value.max);
      writer.writeEndDocument();
    }

    // == Mongo methods ==

    // Codec to use when reading/writing data into bson
    public static final MetricPercentiles CODEC = new MetricPercentiles();
  }

  /** Stage metrics for completed tasks */
  public static class StageMetrics extends AbstractCodec<StageMetrics> {
    public static final String FIELD_NUM_TASKS = "numTasks";
    public static final String FIELD_TASK_DURATION = "taskDuration";
    public static final String FIELD_TASK_DESERIALIZE_TIME = "taskDeserializationTime";
    public static final String FIELD_GC_TIME = "gcTime";
    public static final String FIELD_RESULT_SERIALIZE_TIME = "resultSerializationTime";
    public static final String FIELD_SHUFFLE_FETCH_WAIT_TIME = "shuffleFetchWaitTime";
    public static final String FIELD_SHUFFLE_REMOTE_BYTES_READ = "shuffleRemoteBytesRead";
    public static final String FIELD_SHUFFLE_LOCAL_BYTES_READ = "shuffleLocalBytesRead";
    public static final String FIELD_SHUFFLE_TOTAL_RECORDS_READ = "shuffleTotalRecordsRead";
    public static final String FIELD_SHUFFLE_BYTES_WRITTEN = "shuffleBytesWritten";
    public static final String FIELD_SHUFFLE_WRITE_TIME = "shuffleWriteTime";
    public static final String FIELD_SHUFFLE_RECORDS_WRITTEN = "shuffleRecordsWritten";

    long numTasks;
    // metrics fields
    MetricPercentiles taskDuration;
    MetricPercentiles taskDeserializationTime;
    MetricPercentiles gcTime;
    MetricPercentiles resultSerializationTime;
    // shuffle read metrics
    MetricPercentiles shuffleFetchWaitTime;
    MetricPercentiles shuffleRemoteBytesRead;
    MetricPercentiles shuffleLocalBytesRead;
    MetricPercentiles shuffleTotalRecordsRead;
    // shuffle write metrics
    MetricPercentiles shuffleBytesWritten;
    MetricPercentiles shuffleWriteTime;
    MetricPercentiles shuffleRecordsWritten;

    public StageMetrics() {
      this.numTasks = 0L;
      this.taskDuration = new MetricPercentiles();
      this.taskDeserializationTime = new MetricPercentiles();
      this.gcTime = new MetricPercentiles();
      this.resultSerializationTime = new MetricPercentiles();
      this.shuffleFetchWaitTime = new MetricPercentiles();
      this.shuffleRemoteBytesRead = new MetricPercentiles();
      this.shuffleLocalBytesRead = new MetricPercentiles();
      this.shuffleTotalRecordsRead = new MetricPercentiles();
      this.shuffleBytesWritten = new MetricPercentiles();
      this.shuffleWriteTime = new MetricPercentiles();
      this.shuffleRecordsWritten = new MetricPercentiles();
    }

    public void setSummary(List<Task> tasks) {
      // set number of tasks that are used to generate summary
      this.numTasks = tasks.size();

      long[] values = new long[tasks.size()];
      int index = 0;
      // build task duration
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getDuration();
      }
      taskDuration.setQuantiles(values);

      // build task deserialization time
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getExecutorMetrics().get(Metrics.EXECUTOR_DESERIALIZE_TIME);
      }
      taskDeserializationTime.setQuantiles(values);

      // build gc time
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().getJvmGcTime();
      }
      gcTime.setQuantiles(values);

      // build result serialization time
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().getResultSerializationTime();
      }
      resultSerializationTime.setQuantiles(values);

      // build shuffle fetch wait time
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleReadMetrics().get(Metrics.SHUFFLE_FETCH_WAIT_TIME);
      }
      shuffleFetchWaitTime.setQuantiles(values);

      // build shuffle remote bytes read
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleReadMetrics().get(Metrics.SHUFFLE_REMOTE_BYTES_READ);
      }
      shuffleRemoteBytesRead.setQuantiles(values);

      // build shuffle local bytes read
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleReadMetrics().get(Metrics.SHUFFLE_LOCAL_BYTES_READ);
      }
      shuffleLocalBytesRead.setQuantiles(values);

      // build shuffle total records read
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleReadMetrics().get(Metrics.SHUFFLE_TOTAL_RECORDS_READ);
      }
      shuffleTotalRecordsRead.setQuantiles(values);

      // build shuffle bytes written
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleWriteMetrics().get(Metrics.SHUFFLE_BYTES_WRITTEN);
      }
      shuffleBytesWritten.setQuantiles(values);

      // build write time
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleWriteMetrics().get(Metrics.SHUFFLE_WRITE_TIME);
      }
      shuffleWriteTime.setQuantiles(values);

      // build shuffle records written
      for (int i = 0; i < values.length; i++) {
        values[i] = tasks.get(i).getMetrics().
          getShuffleWriteMetrics().get(Metrics.SHUFFLE_RECORDS_WRITTEN);
      }
      shuffleRecordsWritten.setQuantiles(values);
    }

    // == Codec methods ==

    @Override
    public StageMetrics decode(BsonReader reader, DecoderContext decoderContext) {
      StageMetrics metrics = new StageMetrics();
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        switch (reader.readName()) {
          case FIELD_NUM_TASKS:
            metrics.numTasks = reader.readInt64();
            break;
          case FIELD_TASK_DURATION:
            metrics.taskDuration = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_TASK_DESERIALIZE_TIME:
            metrics.taskDeserializationTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_GC_TIME:
            metrics.gcTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_RESULT_SERIALIZE_TIME:
            metrics.resultSerializationTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_FETCH_WAIT_TIME:
            metrics.shuffleFetchWaitTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_REMOTE_BYTES_READ:
            metrics.shuffleRemoteBytesRead = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_LOCAL_BYTES_READ:
            metrics.shuffleLocalBytesRead = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_TOTAL_RECORDS_READ:
            metrics.shuffleTotalRecordsRead = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_BYTES_WRITTEN:
            metrics.shuffleBytesWritten = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_WRITE_TIME:
            metrics.shuffleWriteTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
            break;
          case FIELD_SHUFFLE_RECORDS_WRITTEN:
            metrics.shuffleRecordsWritten = MetricPercentiles.CODEC.decode(reader, decoderContext);
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
    public Class<StageMetrics> getEncoderClass() {
      return StageMetrics.class;
    }

    @Override
    public void encode(BsonWriter writer, StageMetrics value, EncoderContext encoderContext) {
      writer.writeStartDocument();
      writer.writeInt64(FIELD_NUM_TASKS, value.numTasks);
      writer.writeName(FIELD_TASK_DURATION);
      MetricPercentiles.CODEC.encode(writer, value.taskDuration, encoderContext);
      writer.writeName(FIELD_TASK_DESERIALIZE_TIME);
      MetricPercentiles.CODEC.encode(writer, value.taskDeserializationTime, encoderContext);
      writer.writeName(FIELD_GC_TIME);
      MetricPercentiles.CODEC.encode(writer, value.gcTime, encoderContext);
      writer.writeName(FIELD_RESULT_SERIALIZE_TIME);
      MetricPercentiles.CODEC.encode(writer, value.resultSerializationTime, encoderContext);
      writer.writeName(FIELD_SHUFFLE_FETCH_WAIT_TIME);
      MetricPercentiles.CODEC.encode(writer, value.shuffleFetchWaitTime, encoderContext);
      writer.writeName(FIELD_SHUFFLE_REMOTE_BYTES_READ);
      MetricPercentiles.CODEC.encode(writer, value.shuffleRemoteBytesRead, encoderContext);
      writer.writeName(FIELD_SHUFFLE_LOCAL_BYTES_READ);
      MetricPercentiles.CODEC.encode(writer, value.shuffleLocalBytesRead, encoderContext);
      writer.writeName(FIELD_SHUFFLE_TOTAL_RECORDS_READ);
      MetricPercentiles.CODEC.encode(writer, value.shuffleTotalRecordsRead, encoderContext);
      writer.writeName(FIELD_SHUFFLE_BYTES_WRITTEN);
      MetricPercentiles.CODEC.encode(writer, value.shuffleBytesWritten, encoderContext);
      writer.writeName(FIELD_SHUFFLE_WRITE_TIME);
      MetricPercentiles.CODEC.encode(writer, value.shuffleWriteTime, encoderContext);
      writer.writeName(FIELD_SHUFFLE_RECORDS_WRITTEN);
      MetricPercentiles.CODEC.encode(writer, value.shuffleRecordsWritten, encoderContext);
      writer.writeEndDocument();
    }

    // == Mongo methods ==

    // Codec to use when reading/writing data into bson
    public static final StageMetrics CODEC = new StageMetrics();
  }

  /** Stage metrics aggregated by executor */
  public static class ExecutorMetrics {
    public static final String FIELD_EXECUTOR_ID = "executorId";
    public static final String FIELD_TASK_TIME = "taskTime";
    public static final String FIELD_TOTAL_TASKS = "totalTasks";
    public static final String FIELD_RUNNING_TASKS = "runningTasks";
    public static final String FIELD_FAILED_TASKS = "failedTasks";
    public static final String FIELD_KILLED_TASKS = "killedTasks";
    public static final String FIELD_SUCCEEDED_TASKS = "succeededTasks";
    public static final String FIELD_SHUFFLE_REMOTE_BYTES_READ = "shuffleRemoteBytesRead";
    public static final String FIELD_SHUFFLE_LOCAL_BYTES_READ = "shuffleLocalBytesRead";
    public static final String FIELD_SHUFFLE_TOTAL_RECORDS_READ = "shuffleTotalRecordsRead";
    public static final String FIELD_SHUFFLE_BYTES_WRITTEN = "shuffleBytesWritten";
    public static final String FIELD_SHUFFLE_RECORDS_WRITTEN = "shuffleRecordsWritten";

    String executorId;
    // metrics
    long taskTime;
    long totalTasks;
    long runningTasks;
    long failedTasks;
    long killedTasks;
    long succeededTasks;
    long shuffleRemoteBytesRead;
    long shuffleLocalBytesRead;
    long shuffleTotalRecordsRead;
    long shuffleBytesWritten;
    long shuffleRecordsWritten;

    public ExecutorMetrics() {
      this.executorId = null;
      this.taskTime = 0L;
      this.totalTasks = 0L;
      this.runningTasks = 0L;
      this.failedTasks = 0L;
      this.killedTasks = 0L;
      this.succeededTasks = 0L;
      this.shuffleRemoteBytesRead = 0L;
      this.shuffleLocalBytesRead = 0L;
      this.shuffleTotalRecordsRead = 0L;
      this.shuffleBytesWritten = 0L;
      this.shuffleRecordsWritten = 0L;
    }

    public void incSummary(Task task) {
      // update task counts
      switch (task.getStatus()) {
        case RUNNING:
          runningTasks++;
          break;
        case FAILED:
          failedTasks++;
          break;
        case KILLED:
          killedTasks++;
          break;
        case SUCCESS:
          succeededTasks++;
          break;
        default:
          break;
      }
      totalTasks++;

      // update metrics
      // duration can be negative in case of running tasks, we discard those
      if (task.getDuration() >= 0) {
        taskTime += task.getDuration();
      }
      shuffleRemoteBytesRead +=
        task.getMetrics().getShuffleReadMetrics().get(Metrics.SHUFFLE_REMOTE_BYTES_READ);
      shuffleLocalBytesRead +=
        task.getMetrics().getShuffleReadMetrics().get(Metrics.SHUFFLE_LOCAL_BYTES_READ);
      shuffleTotalRecordsRead +=
        task.getMetrics().getShuffleReadMetrics().get(Metrics.SHUFFLE_TOTAL_RECORDS_READ);
      shuffleBytesWritten +=
        task.getMetrics().getShuffleWriteMetrics().get(Metrics.SHUFFLE_BYTES_WRITTEN);
      shuffleRecordsWritten +=
        task.getMetrics().getShuffleWriteMetrics().get(Metrics.SHUFFLE_RECORDS_WRITTEN);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof ExecutorMetrics)) return false;
      ExecutorMetrics that = (ExecutorMetrics) obj;
      return
        (this.executorId == null && that.executorId == null ||
          this.executorId.equals(that.executorId)) &&
        this.taskTime == that.taskTime &&
        this.totalTasks == that.totalTasks &&
        this.runningTasks == that.runningTasks &&
        this.failedTasks == that.failedTasks &&
        this.killedTasks == that.killedTasks &&
        this.succeededTasks == that.succeededTasks &&
        this.shuffleRemoteBytesRead == that.shuffleRemoteBytesRead &&
        this.shuffleLocalBytesRead == that.shuffleLocalBytesRead &&
        this.shuffleTotalRecordsRead == that.shuffleTotalRecordsRead &&
        this.shuffleBytesWritten == that.shuffleBytesWritten &&
        this.shuffleRecordsWritten == that.shuffleRecordsWritten;
    }

    // == Mongo methods ==

    // Codec to use when reading/writing data into bson
    static final ReadEncoder<ExecutorMetrics> READ_ENCODER = new ReadEncoder<ExecutorMetrics>() {
      @Override
      public ExecutorMetrics read(BsonReader reader) {
        ExecutorMetrics summary = new ExecutorMetrics();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
          switch (reader.readName()) {
            case FIELD_EXECUTOR_ID:
              summary.executorId = safeReadString(reader);
              break;
            case FIELD_TASK_TIME:
              summary.taskTime = reader.readInt64();
              break;
            case FIELD_TOTAL_TASKS:
              summary.totalTasks = reader.readInt64();
              break;
            case FIELD_RUNNING_TASKS:
              summary.runningTasks = reader.readInt64();
              break;
            case FIELD_FAILED_TASKS:
              summary.failedTasks = reader.readInt64();
              break;
            case FIELD_KILLED_TASKS:
              summary.killedTasks = reader.readInt64();
              break;
            case FIELD_SUCCEEDED_TASKS:
              summary.succeededTasks = reader.readInt64();
              break;
            case FIELD_SHUFFLE_REMOTE_BYTES_READ:
              summary.shuffleRemoteBytesRead = reader.readInt64();
              break;
            case FIELD_SHUFFLE_LOCAL_BYTES_READ:
              summary.shuffleLocalBytesRead = reader.readInt64();
              break;
            case FIELD_SHUFFLE_TOTAL_RECORDS_READ:
              summary.shuffleTotalRecordsRead = reader.readInt64();
              break;
            case FIELD_SHUFFLE_BYTES_WRITTEN:
              summary.shuffleBytesWritten = reader.readInt64();
              break;
            case FIELD_SHUFFLE_RECORDS_WRITTEN:
              summary.shuffleRecordsWritten = reader.readInt64();
              break;
            default:
              reader.skipValue();
              break;
          }
        }
        reader.readEndDocument();
        return summary;
      }
    };

    static final WriteEncoder<ExecutorMetrics> WRITE_ENCODER = new WriteEncoder<ExecutorMetrics>() {
      @Override
      public void write(BsonWriter writer, ExecutorMetrics value) {
        writer.writeStartDocument();
        safeWriteString(writer, FIELD_EXECUTOR_ID, value.executorId);
        writer.writeInt64(FIELD_TASK_TIME, value.taskTime);
        writer.writeInt64(FIELD_TOTAL_TASKS, value.totalTasks);
        writer.writeInt64(FIELD_RUNNING_TASKS, value.runningTasks);
        writer.writeInt64(FIELD_FAILED_TASKS, value.failedTasks);
        writer.writeInt64(FIELD_KILLED_TASKS, value.killedTasks);
        writer.writeInt64(FIELD_SUCCEEDED_TASKS, value.succeededTasks);
        writer.writeInt64(FIELD_SHUFFLE_REMOTE_BYTES_READ, value.shuffleRemoteBytesRead);
        writer.writeInt64(FIELD_SHUFFLE_LOCAL_BYTES_READ, value.shuffleLocalBytesRead);
        writer.writeInt64(FIELD_SHUFFLE_TOTAL_RECORDS_READ, value.shuffleTotalRecordsRead);
        writer.writeInt64(FIELD_SHUFFLE_BYTES_WRITTEN, value.shuffleBytesWritten);
        writer.writeInt64(FIELD_SHUFFLE_RECORDS_WRITTEN, value.shuffleRecordsWritten);
        writer.writeEndDocument();
      }
    };
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_STAGE_ID = "stageId";
  public static final String FIELD_STAGE_ATTEMPT_ID = "stageAttemptId";
  public static final String FIELD_STAGE_METRICS = "stageMetrics";
  public static final String FIELD_EXECUTOR_METRICS = "executorMetrics";

  private String appId;
  private int stageId;
  private int stageAttemptId;
  StageMetrics stageMetrics;
  HashMap<String, ExecutorMetrics> executors;

  public StageSummary() {
    this.appId = null;
    this.stageId = -1;
    this.stageAttemptId = -1;

    this.stageMetrics = new StageMetrics();
    this.executors = new HashMap<String, ExecutorMetrics>();
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getStageAttemptId() {
    return stageAttemptId;
  }

  // == Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setStageId(int value) {
    this.stageId = value;
  }

  public void setStageAttemptId(int value) {
    this.stageAttemptId = value;
  }

  /** Set summary from list of tasks */
  public void setSummary(List<Task> tasks) {
    // get summary for completed tasks
    List<Task> completedTasks = new ArrayList<Task>();
    for (Task task : tasks) {
      if (task.getStatus() == Task.Status.SUCCESS) {
        completedTasks.add(task);
      }
    }
    stageMetrics.setSummary(completedTasks);

    // get executor metrics
    for (Task task : tasks) {
      if (!executors.containsKey(task.getExecutorId())) {
        executors.put(task.getExecutorId(), new ExecutorMetrics());
      }
      executors.get(task.getExecutorId()).incSummary(task);
    }
  }

  // == Codec methods ==

  @Override
  public StageSummary decode(BsonReader reader, DecoderContext decoderContext) {
    StageSummary summary = new StageSummary();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          summary.setAppId(safeReadString(reader));
          break;
        case FIELD_STAGE_ID:
          summary.setStageId(reader.readInt32());
          break;
        case FIELD_STAGE_ATTEMPT_ID:
          summary.setStageAttemptId(reader.readInt32());
          break;
        case FIELD_STAGE_METRICS:
          summary.stageMetrics = StageMetrics.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_EXECUTOR_METRICS:
          summary.executors = readMap(reader, ExecutorMetrics.READ_ENCODER);
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return summary;
  }

  @Override
  public Class<StageSummary> getEncoderClass() {
    return StageSummary.class;
  }

  @Override
  public void encode(BsonWriter writer, StageSummary value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    writer.writeInt32(FIELD_STAGE_ID, value.getStageId());
    writer.writeInt32(FIELD_STAGE_ATTEMPT_ID, value.getStageAttemptId());
    writer.writeName(FIELD_STAGE_METRICS);
    StageMetrics.CODEC.encode(writer, value.stageMetrics, encoderContext);
    writeMap(writer, FIELD_EXECUTOR_METRICS, value.executors, ExecutorMetrics.WRITE_ENCODER);
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static StageSummary getOrCreate(
      MongoClient client, String appId, int stageId, int attempt) {
    StageSummary summary = Mongo.stageSummary(client).find(
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_STAGE_ID, stageId),
        Filters.eq(FIELD_STAGE_ATTEMPT_ID, attempt)
      )).first();
    if (summary == null) {
      summary = new StageSummary();
      summary.setAppId(appId);
      summary.setStageId(stageId);
      summary.setStageAttemptId(attempt);
    }
    summary.setMongoClient(client);
    return summary;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null || stageId < 0 || stageAttemptId < 0) return;
    Mongo.upsertOne(
      Mongo.stageSummary(client),
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_STAGE_ID, stageId),
        Filters.eq(FIELD_STAGE_ATTEMPT_ID, stageAttemptId)
      ),
      this
    );
  }
}
