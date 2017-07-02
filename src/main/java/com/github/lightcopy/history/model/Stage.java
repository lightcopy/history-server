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

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.github.lightcopy.history.event.StageInfo;

public class Stage extends AbstractCodec<Stage> {
  // stage lifecycle status
  public enum Status {
    UNKNOWN, PENDING, SKIPPED, ACTIVE, COMPLETED, FAILED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_JOB_ID = "jobId";
  // unique stage id within application, a combination of stage id and attempt id
  public static final String FIELD_UNIQUE_STAGE_ID = "uniqueStageId";
  public static final String FIELD_STAGE_ID = "stageId";
  public static final String FIELD_STAGE_ATTEMPT_ID = "stageAttemptId";
  public static final String FIELD_STAGE_NAME = "stageName";
  public static final String FIELD_ACTIVE_TASKS = "activeTasks";
  public static final String FIELD_COMPLETED_TASKS = "completedTasks";
  public static final String FIELD_FAILED_TASKS = "failedTasks";
  public static final String FIELD_TOTAL_TASKS = "totalTasks";
  public static final String FIELD_DETAILS = "details";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_FAILURE_REASON = "failureReason";
  public static final String FIELD_METRICS = "metrics";

  private String appId;
  private int jobId;
  private long uniqueStageId;
  private int stageId;
  private int stageAttemptId;
  private String stageName;

  // task info
  private int activeTasks;
  private int completedTasks;
  private int failedTasks;
  private int totalTasks;

  private String details;
  private long starttime;
  private long endtime;
  private long duration;
  private Status status;
  private String failureReason;
  private Metrics metrics;

  public Stage() {
    this.appId = null;
    this.jobId = -1;
    this.uniqueStageId = -1;
    this.stageId = -1;
    this.stageAttemptId = -1;
    this.stageName = null;
    this.activeTasks = -1;
    this.completedTasks = -1;
    this.failedTasks = -1;
    this.totalTasks = -1;
    this.details = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.UNKNOWN;
    this.failureReason = null;
    this.metrics = new Metrics();
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public int getJobId() {
    return jobId;
  }

  public long getUniqueStageId() {
    return uniqueStageId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getStageAttemptId() {
    return stageAttemptId;
  }

  public String getStageName() {
    return stageName;
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

  public String getDetails() {
    return details;
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

  public Metrics getMetrics() {
    return metrics;
  }

  // == Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setJobId(int value) {
    this.jobId = value;
  }

  public void setUniqueStageId(long value) {
    this.uniqueStageId = value;
  }

  public void setStageId(int value) {
    this.stageId = value;
  }

  public void setStageAttemptId(int value) {
    this.stageAttemptId = value;
  }

  public void setStageName(String value) {
    this.stageName = value;
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

  public void setDetails(String value) {
    this.details = value;
  }

  public void setStartTime(long value) {
    this.starttime = value;
  }

  public void setEndTime(long value) {
    this.endtime = value;
  }

  public void setDuration(long value) {
    this.duration = value;
  }

  public void setStatus(Status value) {
    this.status = value;
  }

  public void setFailureReason(String value) {
    this.failureReason = value;
  }

  public void setMetrics(Metrics value) {
    this.metrics = value;
  }

  /**
   * Update stage from StageInfo.
   * @param info stage info
   */
  public void update(StageInfo info) {
    setStageId(info.stageId);
    setStageAttemptId(info.stageAttemptId);
    // construct unique stage id, upper 32 bits - stage id, and the lower 32 bits - attempt id
    long stageIdBits = (long) info.stageId;
    long attempIdBits = (long) info.stageAttemptId;
    setUniqueStageId((stageIdBits << 32) | attempIdBits);
    setStageName(info.stageName);
    setTotalTasks(info.numTasks);
    setDetails(info.details);
    setStartTime((info.submissionTime <= 0) ? -1L : info.submissionTime);
    setEndTime((info.completionTime <= 0) ? -1L : info.completionTime);
    setFailureReason(info.failureReason);
    if (starttime >= 0 && endtime >= starttime) {
      setDuration(endtime - starttime);
    }
  }

  /**
   * Update metrics for stage using provided delta update.
   * Stage metrics will be updated incrementally based on update.
   * @param update metrics delta
   */
  public void updateMetrics(Metrics update) {
    this.metrics.merge(update);
  }

  // == Codec methods ==

  @Override
  public Stage decode(BsonReader reader, DecoderContext decoderContext) {
    Stage stage = new Stage();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          stage.setAppId(safeReadString(reader));
          break;
        case FIELD_JOB_ID:
          stage.setJobId(reader.readInt32());
          break;
        case FIELD_UNIQUE_STAGE_ID:
          stage.setUniqueStageId(reader.readInt64());
          break;
        case FIELD_STAGE_ID:
          stage.setStageId(reader.readInt32());
          break;
        case FIELD_STAGE_ATTEMPT_ID:
          stage.setStageAttemptId(reader.readInt32());
          break;
        case FIELD_STAGE_NAME:
          stage.setStageName(safeReadString(reader));
          break;
        case FIELD_ACTIVE_TASKS:
          stage.setActiveTasks(reader.readInt32());
          break;
        case FIELD_COMPLETED_TASKS:
          stage.setCompletedTasks(reader.readInt32());
          break;
        case FIELD_FAILED_TASKS:
          stage.setFailedTasks(reader.readInt32());
          break;
        case FIELD_TOTAL_TASKS:
          stage.setTotalTasks(reader.readInt32());
          break;
        case FIELD_DETAILS:
          stage.setDetails(safeReadString(reader));
          break;
        case FIELD_STARTTIME:
          stage.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          stage.setEndTime(reader.readInt64());
          break;
        case FIELD_DURATION:
          stage.setDuration(reader.readInt64());
          break;
        case FIELD_STATUS:
          stage.setStatus(Status.valueOf(safeReadString(reader)));
          break;
        case FIELD_FAILURE_REASON:
          stage.setFailureReason(safeReadString(reader));
          break;
        case FIELD_METRICS:
          stage.setMetrics(stage.getMetrics().decode(reader, decoderContext));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return stage;
  }

  @Override
  public Class<Stage> getEncoderClass() {
    return Stage.class;
  }

  @Override
  public void encode(BsonWriter writer, Stage value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    writer.writeInt32(FIELD_JOB_ID, value.getJobId());
    writer.writeInt64(FIELD_UNIQUE_STAGE_ID, value.getUniqueStageId());
    writer.writeInt32(FIELD_STAGE_ID, value.getStageId());
    writer.writeInt32(FIELD_STAGE_ATTEMPT_ID, value.getStageAttemptId());
    safeWriteString(writer, FIELD_STAGE_NAME, value.getStageName());
    writer.writeInt32(FIELD_ACTIVE_TASKS, value.getActiveTasks());
    writer.writeInt32(FIELD_COMPLETED_TASKS, value.getCompletedTasks());
    writer.writeInt32(FIELD_FAILED_TASKS, value.getFailedTasks());
    writer.writeInt32(FIELD_TOTAL_TASKS, value.getTotalTasks());
    safeWriteString(writer, FIELD_DETAILS, value.getDetails());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    safeWriteString(writer, FIELD_FAILURE_REASON, value.getFailureReason());
    writer.writeName(FIELD_METRICS);
    value.getMetrics().encode(writer, value.getMetrics(), encoderContext);
    writer.writeEndDocument();
  }
}