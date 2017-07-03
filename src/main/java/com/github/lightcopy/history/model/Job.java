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

public class Job extends AbstractCodec<Job> {
  // job lifecycle status
  public enum Status {
    UNKNOWN, RUNNING, SUCCEEDED, FAILED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_JOB_ID = "jobId";
  public static final String FIELD_JOB_NAME = "jobName";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_ERROR_DESCRIPTION = "errorDescription";
  public static final String FIELD_ERROR_DETAILS = "errorDetails";
  public static final String FIELD_ACTIVE_TASKS = "activeTasks";
  public static final String FIELD_COMPLETED_TASKS = "completedTasks";
  public static final String FIELD_FAILED_TASKS = "failedTasks";
  public static final String FIELD_TOTAL_TASKS = "totalTasks";
  public static final String FIELD_METRICS = "metrics";

  private String appId;
  private int jobId;
  private String jobName;
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
  private int totalTasks;
  private Metrics metrics;

  public Job() {
    this.appId = null;
    this.jobName = null;
    this.jobId = -1;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.UNKNOWN;
    this.errorDescription = null;
    this.errorDetails = null;
    this.activeTasks = -1;
    this.completedTasks = -1;
    this.failedTasks = -1;
    this.totalTasks = -1;
    this.metrics = new Metrics();
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

  public int getTotalTasks() {
    return this.totalTasks;
  }

  public Metrics getMetrics() {
    return this.metrics;
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

  public void setStartTime(long value) {
    this.starttime = value;
  }

  public void setEndTime(long value) {
    this.endtime = value;
  }

  public void setDuration(long value) {
    this.duration = value;
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

  public void setTotalTasks(int value) {
    this.totalTasks = value;
  }

  public void setMetrics(Metrics value) {
    this.metrics = value;
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
        case FIELD_TOTAL_TASKS:
          job.setTotalTasks(reader.readInt32());
          break;
        case FIELD_METRICS:
          job.setMetrics(job.getMetrics().decode(reader, decoderContext));
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
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    safeWriteString(writer, FIELD_ERROR_DESCRIPTION, value.getErrorDescription());
    safeWriteString(writer, FIELD_ERROR_DETAILS, value.getErrorDetails());
    writer.writeInt32(FIELD_ACTIVE_TASKS, value.getActiveTasks());
    writer.writeInt32(FIELD_COMPLETED_TASKS, value.getCompletedTasks());
    writer.writeInt32(FIELD_FAILED_TASKS, value.getFailedTasks());
    writer.writeInt32(FIELD_TOTAL_TASKS, value.getTotalTasks());
    writer.writeName(FIELD_METRICS);
    value.getMetrics().encode(writer, value.getMetrics(), encoderContext);
    writer.writeEndDocument();
  }
}
