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

import java.util.HashSet;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.github.lightcopy.history.Mongo;

public class Job extends AbstractCodec<Job> {
  // job lifecycle status
  public enum Status {
    UNKNOWN, RUNNING, SUCCEEDED, FAILED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_JOB_ID = "jobId";
  public static final String FIELD_JOB_NAME = "jobName";
  public static final String FIELD_JOB_GROUP = "jobGroup";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_ERROR_DESCRIPTION = "errorDescription";
  public static final String FIELD_ERROR_DETAILS = "errorDetails";

  public static final String FIELD_ACTIVE_TASKS = "activeTasks";
  public static final String FIELD_COMPLETED_TASKS = "completedTasks";
  public static final String FIELD_FAILED_TASKS = "failedTasks";
  public static final String FIELD_SKIPPED_TASKS = "skippedTasks";
  public static final String FIELD_TOTAL_TASKS = "totalTasks";
  public static final String FIELD_METRICS = "metrics";

  public static final String FIELD_PENDING_STAGES = "pendingStages";
  public static final String FIELD_ACTIVE_STAGES = "activeStages";
  public static final String FIELD_COMPLETED_STAGES = "completedStages";
  public static final String FIELD_FAILED_STAGES = "failedStages";
  public static final String FIELD_SKIPPED_STAGES = "skippedStages";

  private String appId;
  private int jobId;
  private String jobName;
  private String jobGroup;
  private long starttime;
  private long endtime;
  private long duration;
  private Status status;
  // only set if status is FAILED
  private String errorDescription;
  private String errorDetails;
  // task info + aggregated metrics
  private int activeTasks;
  private int completedTasks;
  private int failedTasks;
  private int skippedTasks;
  private int totalTasks;
  private Metrics metrics;
  // stage info
  private HashSet<Long> pendingStages;
  private HashSet<Long> activeStages;
  private HashSet<Long> completedStages;
  private HashSet<Long> failedStages;
  private HashSet<Long> skippedStages;

  public Job() {
    this.appId = null;
    this.jobName = null;
    this.jobGroup = null;
    this.jobId = -1;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.UNKNOWN;
    this.errorDescription = null;
    this.errorDetails = null;

    // set tasks data to 0, so we can increment/decrement them
    // naturally, without conditions
    this.activeTasks = 0;
    this.completedTasks = 0;
    this.failedTasks = 0;
    this.skippedTasks = 0;
    this.totalTasks = 0;
    this.metrics = new Metrics();

    // set stages to empty sets, so we can easily increment/decrement them
    // stage id is stored as long (stageId << 32 | attempt)
    this.pendingStages = new HashSet<Long>();
    this.activeStages = new HashSet<Long>();
    this.completedStages = new HashSet<Long>();
    this.failedStages = new HashSet<Long>();
    this.skippedStages = new HashSet<Long>();
  }

  // == Getters ==

  public String getAppId() {
    return this.appId;
  }

  public int getJobId() {
    return this.jobId;
  }

  public String getJobName() {
    return this.jobName;
  }

  public String getJobGroup() {
    return this.jobGroup;
  }

  public long getStartTime() {
    return this.starttime;
  }

  public long getEndTime() {
    return this.endtime;
  }

  public long getDuration() {
    return this.duration;
  }

  public Status getStatus() {
    return this.status;
  }

  public String getErrorDescription() {
    return this.errorDescription;
  }

  public String getErrorDetails() {
    return this.errorDetails;
  }

  public int getActiveTasks() {
    return this.activeTasks;
  }

  public int getCompletedTasks() {
    return this.completedTasks;
  }

  public int getFailedTasks() {
    return this.failedTasks;
  }

  public int getSkippedTasks() {
    return this.skippedTasks;
  }

  public int getTotalTasks() {
    return this.totalTasks;
  }

  public Metrics getMetrics() {
    return this.metrics;
  }

  public HashSet<Long> getPendingStages() {
    return pendingStages;
  }

  public HashSet<Long> getActiveStages() {
    return activeStages;
  }

  public HashSet<Long> getCompletedStages() {
    return completedStages;
  }

  public HashSet<Long> getFailedStages() {
    return failedStages;
  }

  public HashSet<Long> getSkippedStages() {
    return skippedStages;
  }

  // == Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setJobId(int value) {
    this.jobId = value;
  }

  public void setJobName(String value) {
    this.jobName = value;
  }

  public void setJobGroup(String value) {
    this.jobGroup = value;
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

  // duration is set based on start/end time
  public void updateDuration() {
    // if both starttime and endtime are valid, we compute duration, otherwise set to -1
    if (starttime >= 0L && endtime >= starttime) {
      setDuration(endtime - starttime);
    } else {
      setDuration(-1L);
    }
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setErrorDescription(String value) {
    this.errorDescription = value;
  }

  public void setErrorDetails(String value) {
    this.errorDetails = value;
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

  public void setSkippedTasks(int value) {
    this.skippedTasks = value;
  }

  public void setTotalTasks(int value) {
    this.totalTasks = value;
  }

  public void setMetrics(Metrics value) {
    this.metrics = value;
  }

  public void setPendingStages(HashSet<Long> value) {
    this.pendingStages = value;
  }

  public void setActiveStages(HashSet<Long> value) {
    this.activeStages = value;
  }

  public void setCompletedStages(HashSet<Long> value) {
    this.completedStages = value;
  }

  public void setFailedStages(HashSet<Long> value) {
    this.failedStages = value;
  }

  public void setSkippedStages(HashSet<Long> value) {
    this.skippedStages = value;
  }

  /**
   * Update metrics for job using provided delta update.
   * Job metrics will be updated incrementally based on update.
   * @param update metrics delta
   */
  public void updateMetrics(Metrics update) {
    this.metrics.merge(update);
  }

  /** Increment active tasks for job */
  public void incActiveTasks() {
    this.activeTasks++;
  }

  /** Decrement active tasks for job */
  public void decActiveTasks() {
    this.activeTasks--;
  }

  /** Increment completed tasks for job */
  public void incCompletedTasks() {
    this.completedTasks++;
  }

  /** Increment failed tasks for job */
  public void incFailedTasks() {
    this.failedTasks++;
  }

  /** Increment skipped tasks by delta for job */
  public void incSkippedTasks(int delta) {
    this.skippedTasks += delta;
  }

  /** Return unique stage id including attempt */
  private static long uniqueStageId(int stageId, int attempt) {
    return ((long) stageId) << 32 | attempt;
  }

  /** Unlink stage from all statuses, stage can be in one set only */
  public void unlinkStage(int stageId, int attempt) {
    long id = uniqueStageId(stageId, attempt);
    pendingStages.remove(id);
    activeStages.remove(id);
    completedStages.remove(id);
    failedStages.remove(id);
    skippedStages.remove(id);
  }

  /** Increment pending stages for job */
  public void markStagePending(int stageId, int attempt) {
    unlinkStage(stageId, attempt);
    this.pendingStages.add(uniqueStageId(stageId, attempt));
  }

  /** Increment active stages for job */
  public void markStageActive(int stageId, int attempt) {
    unlinkStage(stageId, attempt);
    this.activeStages.add(uniqueStageId(stageId, attempt));
  }

  /** Increment completed stages for job */
  public void markStageCompleted(int stageId, int attempt) {
    unlinkStage(stageId, attempt);
    this.completedStages.add(uniqueStageId(stageId, attempt));
  }

  /** Increment failed stages for job */
  public void markStageFailed(int stageId, int attempt) {
    unlinkStage(stageId, attempt);
    this.failedStages.add(uniqueStageId(stageId, attempt));
  }

  /** Increment skipped stages for job */
  public void markStageSkipped(int stageId, int attempt) {
    unlinkStage(stageId, attempt);
    this.skippedStages.add(uniqueStageId(stageId, attempt));
  }

  // == Codec methods ==

  @Override
  public Job decode(BsonReader reader, DecoderContext decoderContext) {
    Job job = new Job();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          job.setAppId(safeReadString(reader));
          break;
        case FIELD_JOB_ID:
          job.setJobId(reader.readInt32());
          break;
        case FIELD_JOB_NAME:
          job.setJobName(safeReadString(reader));
          break;
        case FIELD_JOB_GROUP:
          job.setJobGroup(safeReadString(reader));
          break;
        case FIELD_STARTTIME:
          job.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          job.setEndTime(reader.readInt64());
          break;
        case FIELD_DURATION:
          job.setDuration(reader.readInt64());
          break;
        case FIELD_STATUS:
          job.setStatus(Status.valueOf(safeReadString(reader)));
          break;
        case FIELD_ERROR_DESCRIPTION:
          job.setErrorDescription(safeReadString(reader));
          break;
        case FIELD_ERROR_DETAILS:
          job.setErrorDetails(safeReadString(reader));
          break;
        case FIELD_ACTIVE_TASKS:
          job.setActiveTasks(reader.readInt32());
          break;
        case FIELD_COMPLETED_TASKS:
          job.setCompletedTasks(reader.readInt32());
          break;
        case FIELD_FAILED_TASKS:
          job.setFailedTasks(reader.readInt32());
          break;
        case FIELD_SKIPPED_TASKS:
          job.setSkippedTasks(reader.readInt32());
          break;
        case FIELD_TOTAL_TASKS:
          job.setTotalTasks(reader.readInt32());
          break;
        case FIELD_METRICS:
          job.setMetrics(Metrics.CODEC.decode(reader, decoderContext));
          break;
        case FIELD_PENDING_STAGES:
          job.setPendingStages(readSet(reader, LONG_ENCODER));
          break;
        case FIELD_ACTIVE_STAGES:
          job.setActiveStages(readSet(reader, LONG_ENCODER));
          break;
        case FIELD_COMPLETED_STAGES:
          job.setCompletedStages(readSet(reader, LONG_ENCODER));
          break;
        case FIELD_FAILED_STAGES:
          job.setFailedStages(readSet(reader, LONG_ENCODER));
          break;
        case FIELD_SKIPPED_STAGES:
          job.setSkippedStages(readSet(reader, LONG_ENCODER));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return job;
  }

  @Override
  public Class<Job> getEncoderClass() {
    return Job.class;
  }

  @Override
  public void encode(BsonWriter writer, Job value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    writer.writeInt32(FIELD_JOB_ID, value.getJobId());
    safeWriteString(writer, FIELD_JOB_NAME, value.getJobName());
    safeWriteString(writer, FIELD_JOB_GROUP, value.getJobGroup());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    safeWriteString(writer, FIELD_ERROR_DESCRIPTION, value.getErrorDescription());
    safeWriteString(writer, FIELD_ERROR_DETAILS, value.getErrorDetails());
    writer.writeInt32(FIELD_ACTIVE_TASKS, value.getActiveTasks());
    writer.writeInt32(FIELD_COMPLETED_TASKS, value.getCompletedTasks());
    writer.writeInt32(FIELD_FAILED_TASKS, value.getFailedTasks());
    writer.writeInt32(FIELD_SKIPPED_TASKS, value.getSkippedTasks());
    writer.writeInt32(FIELD_TOTAL_TASKS, value.getTotalTasks());
    writer.writeName(FIELD_METRICS);
    Metrics.CODEC.encode(writer, value.getMetrics(), encoderContext);
    writeSet(writer, FIELD_PENDING_STAGES, value.getPendingStages(), LONG_ENCODER);
    writeSet(writer, FIELD_ACTIVE_STAGES, value.getActiveStages(), LONG_ENCODER);
    writeSet(writer, FIELD_COMPLETED_STAGES, value.getCompletedStages(), LONG_ENCODER);
    writeSet(writer, FIELD_FAILED_STAGES, value.getFailedStages(), LONG_ENCODER);
    writeSet(writer, FIELD_SKIPPED_STAGES, value.getSkippedStages(), LONG_ENCODER);
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static Job getOrCreate(MongoClient client, String appId, int jobId) {
    Job job = Mongo.jobs(client).find(
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_JOB_ID, jobId)
      )).first();
    if (job == null) {
      job = new Job();
      job.setAppId(appId);
      job.setJobId(jobId);
    }
    job.setMongoClient(client);
    return job;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null || jobId < 0) return;
    Mongo.upsertOne(
      Mongo.jobs(client),
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_JOB_ID, jobId)
      ),
      this
    );
  }
}
