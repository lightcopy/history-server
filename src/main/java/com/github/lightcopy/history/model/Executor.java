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

import java.util.Map;
import java.util.HashMap;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.github.lightcopy.history.Mongo;

public class Executor extends AbstractCodec<Executor> {
  public enum Status {
    UNKNOWN, ACTIVE, REMOVED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_EXECUTOR_ID = "executorId";
  public static final String FIELD_SORT_EXECUTOR_ID = "sortExecutorId";
  public static final String FIELD_HOST = "host";
  public static final String FIELD_PORT = "port";
  public static final String FIELD_CORES = "cores";
  public static final String FIELD_MAX_MEMORY = "maxMemory";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_FAILURE_REASON = "failureReason";
  public static final String FIELD_LOGS = "logs";

  public static final String FIELD_ACTIVE_TASKS = "activeTasks";
  public static final String FIELD_COMPLETED_TASKS = "completedTasks";
  public static final String FIELD_FAILED_TASKS = "failedTasks";
  public static final String FIELD_TOTAL_TASKS = "totalTasks";
  public static final String FIELD_TASK_TIME = "taskTime";
  public static final String FIELD_METRICS = "metrics";

  private String appId;
  private String executorId;
  // executor id for sorting only, is not used to query data
  // we either convert executor id into integer or use -1, e.g. "driver"
  // TODO: make it unique, so we can index by this field
  private int sortExecutorId;
  private String host;
  private int port;
  private int cores;
  private long maxMemory;
  private long starttime;
  private long endtime;
  private long duration;
  private Status status;
  private String failureReason;
  // map containing "stdout" and "stderr" keys with urls
  private HashMap<String, String> logs;
  // task info + aggregated metrics
  private int activeTasks;
  private int completedTasks;
  private int failedTasks;
  private int totalTasks;
  private long taskTime;
  private Metrics metrics;

  public Executor() {
    this.appId = null;
    this.executorId = null;
    // set as largest, so "driver" appears at the end
    this.sortExecutorId = Integer.MAX_VALUE;
    this.host = null;
    this.port = -1;
    this.cores = -1;
    this.maxMemory = -1L;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.UNKNOWN;
    // this is only set if executor was removed
    this.failureReason = null;
    this.logs = new HashMap<String, String>();

    this.activeTasks = 0;
    this.completedTasks = 0;
    this.failedTasks = 0;
    this.totalTasks = 0;
    this.taskTime = 0L;
    this.metrics = new Metrics();
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public String getExecutorId() {
    return executorId;
  }

  // executor id that is used for sorting
  public int getSortExecutorId() {
    return sortExecutorId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getCores() {
    return cores;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public long getStartTime() {
    return starttime;
  }

  public long getEndTime() {
    return endtime;
  }

  public long getDuration() {
    return duration;
  }

  public Status getStatus() {
    return status;
  }

  public String getFailureReason() {
    return failureReason;
  }

  public HashMap<String, String> getLogs() {
    return logs;
  }

  public String getStdoutUrl() {
    return logs.get("stdout");
  }

  public String getStderrUrl() {
    return logs.get("stderr");
  }

  public int getActiveTasks() {
    return activeTasks;
  }

  public int getCompletedTasks() {
    return completedTasks;
  }

  public int getFailedTasks() {
    return failedTasks;
  }

  public int getTotalTasks() {
    return totalTasks;
  }

  public long getTaskTime() {
    return taskTime;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  // == Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setExecutorId(String value) {
    this.executorId = value;
  }

  // this should be used internally only
  protected void setSortExecutorId(int value) {
    this.sortExecutorId = value;
  }

  /**
   * Convert executor id into sorted integer column.
   * "driver" is always assigned -1.
   */
  protected void updateSortExecutorId() {
    if (this.executorId == null || this.executorId.equals("driver")) {
      this.sortExecutorId = Integer.MAX_VALUE;
    } else {
      try {
        this.sortExecutorId = Integer.parseInt(this.executorId);
      } catch (NumberFormatException err) {
        this.sortExecutorId = Integer.MAX_VALUE;
      }
    }
  }

  public void setHost(String value) {
    this.host = value;
  }

  public void setPort(int value) {
    this.port = value;
  }

  public void setCores(int value) {
    this.cores = value;
  }

  public void setMaxMemory(long value) {
    this.maxMemory = value;
  }

  public void setStartTime(long value) {
    this.starttime = value;
  }

  public void setEndTime(long value) {
    this.endtime = value;
  }

  // duration is set based on start/end time
  public void updateDuration() {
    // if both starttime and endtime are valid, we compute duration, otherwise set to -1
    if (starttime >= 0L && endtime >= starttime) {
      setDuration(endtime - starttime);
    } else {
      setDuration(-1L);
    }
  }

  // method to set duration during deserialization
  private void setDuration(long value) {
    this.duration = value;
  }

  public void setStatus(Status value) {
    this.status = value;
  }

  public void setFailureReason(String value) {
    this.failureReason = value;
  }

  public void setLogs(Map<String, String> value) {
    this.logs = new HashMap<String, String>(value);
  }

  public void setActiveTasks(int value) {
    this.activeTasks = value;
  }

  public void setCompletedTasks(int value) {
    this.completedTasks = value;
  }

  public void setFailedTasks(int value) {
    this.failedTasks = value;
  }

  public void setTotalTasks(int value) {
    this.totalTasks = value;
  }

  public void setTaskTime(long value) {
    this.taskTime = value;
  }

  public void setMetrics(Metrics value) {
    this.metrics = value;
  }

  /** Increment active tasks for executor */
  public void incActiveTasks() {
    this.activeTasks++;
    updateTotalTasks();
  }

  /** Decrement active tasks for executor */
  public void decActiveTasks() {
    this.activeTasks--;
    updateTotalTasks();
  }

  /** Increment completed tasks for executor */
  public void incCompletedTasks() {
    this.completedTasks++;
    updateTotalTasks();
  }

  /** Increment failed tasks for executor */
  public void incFailedTasks() {
    this.failedTasks++;
    updateTotalTasks();
  }

  /** Increment total task time by duration, only if duration is non-negative */
  public void incTaskTime(long duration) {
    if (duration >= 0) {
      this.taskTime += duration;
    }
  }

  /** Compute total processed tasks */
  private void updateTotalTasks() {
    this.totalTasks = this.activeTasks + this.completedTasks + this.failedTasks;
  }

  /**
   * Update metrics for executor based on provided delta.
   * @param delta incremental update
   */
  public void updateMetrics(Metrics delta) {
    this.metrics.merge(delta);
  }

  // == Codec methods ==

  @Override
  public Executor decode(BsonReader reader, DecoderContext decoderContext) {
    Executor exc = new Executor();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          exc.setAppId(safeReadString(reader));
          break;
        case FIELD_EXECUTOR_ID:
          exc.setExecutorId(safeReadString(reader));
          break;
        case FIELD_SORT_EXECUTOR_ID:
          exc.setSortExecutorId(reader.readInt32());
          break;
        case FIELD_HOST:
          exc.setHost(safeReadString(reader));
          break;
        case FIELD_PORT:
          exc.setPort(reader.readInt32());
          break;
        case FIELD_CORES:
          exc.setCores(reader.readInt32());
          break;
        case FIELD_MAX_MEMORY:
          exc.setMaxMemory(reader.readInt64());
          break;
        case FIELD_STARTTIME:
          exc.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          exc.setEndTime(reader.readInt64());
          break;
        case FIELD_DURATION:
          exc.setDuration(reader.readInt64());
          break;
        case FIELD_STATUS:
          exc.setStatus(Status.valueOf(safeReadString(reader)));
          break;
        case FIELD_FAILURE_REASON:
          exc.setFailureReason(safeReadString(reader));
          break;
        case FIELD_LOGS:
          exc.setLogs(readMap(reader,  STRING_ENCODER));
          break;
        case FIELD_ACTIVE_TASKS:
          exc.setActiveTasks(reader.readInt32());
          break;
        case FIELD_COMPLETED_TASKS:
          exc.setCompletedTasks(reader.readInt32());
          break;
        case FIELD_FAILED_TASKS:
          exc.setFailedTasks(reader.readInt32());
          break;
        case FIELD_TOTAL_TASKS:
          exc.setTotalTasks(reader.readInt32());
          break;
        case FIELD_TASK_TIME:
          exc.setTaskTime(reader.readInt64());
          break;
        case FIELD_METRICS:
          exc.setMetrics(Metrics.CODEC.decode(reader, decoderContext));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return exc;
  }

  @Override
  public Class<Executor> getEncoderClass() {
    return Executor.class;
  }

  @Override
  public void encode(BsonWriter writer, Executor value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    safeWriteString(writer, FIELD_EXECUTOR_ID, value.getExecutorId());
    writer.writeInt32(FIELD_SORT_EXECUTOR_ID, value.getSortExecutorId());
    safeWriteString(writer, FIELD_HOST, value.getHost());
    writer.writeInt32(FIELD_PORT, value.getPort());
    writer.writeInt32(FIELD_CORES, value.getCores());
    writer.writeInt64(FIELD_MAX_MEMORY, value.getMaxMemory());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    safeWriteString(writer, FIELD_FAILURE_REASON, value.getFailureReason());
    writeMap(writer, FIELD_LOGS, value.getLogs(), STRING_ENCODER);
    writer.writeInt32(FIELD_ACTIVE_TASKS, value.getActiveTasks());
    writer.writeInt32(FIELD_COMPLETED_TASKS, value.getCompletedTasks());
    writer.writeInt32(FIELD_FAILED_TASKS, value.getFailedTasks());
    writer.writeInt32(FIELD_TOTAL_TASKS, value.getTotalTasks());
    writer.writeInt64(FIELD_TASK_TIME, value.getTaskTime());
    writer.writeName(FIELD_METRICS);
    Metrics.CODEC.encode(writer, value.getMetrics(), encoderContext);
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static Executor getOrCreate(MongoClient client, String appId, String executorId) {
    Executor exc = Mongo.executors(client).find(
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_EXECUTOR_ID, executorId)
      )).first();
    if (exc == null) {
      exc = new Executor();
      exc.setAppId(appId);
      exc.setExecutorId(executorId);
      // update executor id here for new executors
      exc.updateSortExecutorId();
    }
    exc.setMongoClient(client);
    return exc;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null || executorId == null) return;
    updateSortExecutorId();

    Mongo.upsertOne(
      Mongo.executors(client),
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_EXECUTOR_ID, executorId)
      ),
      this
    );
  }
}
