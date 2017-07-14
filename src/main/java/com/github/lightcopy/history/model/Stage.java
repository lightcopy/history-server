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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

import com.github.lightcopy.history.Mongo;
import com.github.lightcopy.history.event.StageInfo;

public class Stage extends AbstractCodec<Stage> {
  private static final Logger LOG = LoggerFactory.getLogger(Stage.class);

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
  public static final String FIELD_JOB_GROUP = "jobGroup";
  public static final String FIELD_ACTIVE_TASKS = "activeTasks";
  public static final String FIELD_COMPLETED_TASKS = "completedTasks";
  public static final String FIELD_FAILED_TASKS = "failedTasks";
  public static final String FIELD_TOTAL_TASKS = "totalTasks";
  public static final String FIELD_DETAILS = "details";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_METRICS = "metrics";
  public static final String FIELD_TASK_TIME = "taskTime";
  public static final String FIELD_ERROR_DESCRIPTION = "errorDescription";
  public static final String FIELD_ERROR_DETAILS = "errorDetails";

  private String appId;
  private int jobId;
  private long uniqueStageId;
  private int stageId;
  private int stageAttemptId;
  private String stageName;
  private String jobGroup;

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
  private Metrics metrics;
  private long taskTime;

  private String errorDescription;
  private String errorDetails;

  public Stage() {
    this.appId = null;
    this.jobId = -1;
    this.uniqueStageId = -1;
    this.stageId = -1;
    this.stageAttemptId = -1;
    this.stageName = null;
    this.jobGroup = null;

    // set tasks data to 0, so we can increment/decrement them
    // naturally, without conditions
    this.activeTasks = 0;
    this.completedTasks = 0;
    this.failedTasks = 0;
    this.totalTasks = 0;

    this.details = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.UNKNOWN;
    this.metrics = new Metrics();
    this.taskTime = 0L;
    this.errorDescription = null;
    this.errorDetails = null;
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

  public String getJobGroup() {
    return jobGroup;
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

  public Metrics getMetrics() {
    return metrics;
  }

  public long getTaskTime() {
    return taskTime;
  }

  public String getErrorDescription() {
    return errorDescription;
  }

  public String getErrorDetails() {
    return errorDetails;
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

  public void setJobGroup(String value) {
    this.jobGroup = value;
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

  public void setMetrics(Metrics value) {
    this.metrics = value;
  }

  public void setTaskTime(long value) {
    this.taskTime = value;
  }

  public void setErrorDescription(String value) {
    this.errorDescription = value;
  }

  public void setErrorDetails(String value) {
    this.errorDetails = value;
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
    setErrorDescription(info.getErrorDescription());
    setErrorDetails(info.getErrorDetails());
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

  /** Increment active tasks for stage */
  public void incActiveTasks() {
    this.activeTasks++;
  }

  /** Decrement active tasks for stage */
  public void decActiveTasks() {
    this.activeTasks--;
  }

  /** Increment completed tasks for stage */
  public void incCompletedTasks() {
    this.completedTasks++;
  }

  /** Increment failed tasks for stage */
  public void incFailedTasks() {
    this.failedTasks++;
  }

  /** Increment total task time by duration, only if duration is non-negative */
  public void incTaskTime(long duration) {
    if (duration >= 0) {
      this.taskTime += duration;
    }
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
        case FIELD_JOB_GROUP:
          stage.setJobGroup(safeReadString(reader));
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
        case FIELD_METRICS:
          stage.setMetrics(Metrics.CODEC.decode(reader, decoderContext));
          break;
        case FIELD_TASK_TIME:
          stage.setTaskTime(reader.readInt64());
          break;
        case FIELD_ERROR_DESCRIPTION:
          stage.setErrorDescription(safeReadString(reader));
          break;
        case FIELD_ERROR_DETAILS:
          stage.setErrorDetails(safeReadString(reader));
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
    safeWriteString(writer, FIELD_JOB_GROUP, value.getJobGroup());
    writer.writeInt32(FIELD_ACTIVE_TASKS, value.getActiveTasks());
    writer.writeInt32(FIELD_COMPLETED_TASKS, value.getCompletedTasks());
    writer.writeInt32(FIELD_FAILED_TASKS, value.getFailedTasks());
    writer.writeInt32(FIELD_TOTAL_TASKS, value.getTotalTasks());
    safeWriteString(writer, FIELD_DETAILS, value.getDetails());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    writer.writeName(FIELD_METRICS);
    Metrics.CODEC.encode(writer, value.getMetrics(), encoderContext);
    writer.writeInt64(FIELD_TASK_TIME, value.getTaskTime());
    safeWriteString(writer, FIELD_ERROR_DESCRIPTION, value.getErrorDescription());
    safeWriteString(writer, FIELD_ERROR_DETAILS, value.getErrorDetails());
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static Stage getOrCreate(MongoClient client, String appId, int stageId, int attempt) {
    Stage stage = Mongo.stages(client).find(
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_STAGE_ID, stageId),
        Filters.eq(FIELD_STAGE_ATTEMPT_ID, attempt)
      )).first();
    if (stage == null) {
      stage = new Stage();
      stage.setAppId(appId);
      stage.setStageId(stageId);
      stage.setStageAttemptId(attempt);
    }
    // if stage attempt is not the first, and does not have job link, we search all previous
    // attempts in order to find the latest job id >= 0. If none found, then keep default -1
    if (stage.getStageAttemptId() > 0 && stage.getJobId() < 0) {
      // workaround for final variable
      final int[] maxJobId = new int[]{-1};
      Mongo.stages(client).find(Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_STAGE_ID, stageId)
      )).forEach(new Block<Stage>() {
        @Override
        public void apply(Stage stage) {
          maxJobId[0] = Math.max(maxJobId[0], stage.getJobId());
        }
      });
      LOG.warn("Stage {} ({}) does not have jobId, assign {}",
        stage.getStageId(), stage.getStageAttemptId(), maxJobId[0]);
      stage.setJobId(maxJobId[0]);
    }
    stage.setMongoClient(client);
    return stage;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null || stageId < 0 || stageAttemptId < 0) return;
    Mongo.upsertOne(
      Mongo.stages(client),
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_STAGE_ID, stageId),
        Filters.eq(FIELD_STAGE_ATTEMPT_ID, stageAttemptId)
      ),
      this
    );
  }
}
