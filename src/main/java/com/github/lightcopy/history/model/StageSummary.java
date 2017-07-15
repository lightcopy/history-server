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

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_STAGE_ID = "stageId";
  public static final String FIELD_STAGE_ATTEMPT_ID = "stageAttemptId";

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

  private String appId;
  private int stageId;
  private int stageAttemptId;
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

  public StageSummary() {
    this.appId = null;
    this.stageId = -1;
    this.stageAttemptId = -1;
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
    // set num tasks used to generate summary
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
        case FIELD_NUM_TASKS:
          summary.numTasks = reader.readInt64();
        case FIELD_TASK_DURATION:
          summary.taskDuration = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_TASK_DESERIALIZE_TIME:
          summary.taskDeserializationTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_GC_TIME:
          summary.gcTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_RESULT_SERIALIZE_TIME:
          summary.resultSerializationTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_FETCH_WAIT_TIME:
          summary.shuffleFetchWaitTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_REMOTE_BYTES_READ:
          summary.shuffleRemoteBytesRead = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_LOCAL_BYTES_READ:
          summary.shuffleLocalBytesRead = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_TOTAL_RECORDS_READ:
          summary.shuffleTotalRecordsRead = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_BYTES_WRITTEN:
          summary.shuffleBytesWritten = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_WRITE_TIME:
          summary.shuffleWriteTime = MetricPercentiles.CODEC.decode(reader, decoderContext);
          break;
        case FIELD_SHUFFLE_RECORDS_WRITTEN:
          summary.shuffleRecordsWritten = MetricPercentiles.CODEC.decode(reader, decoderContext);
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
