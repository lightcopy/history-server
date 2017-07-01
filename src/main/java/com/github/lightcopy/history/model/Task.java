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

import com.github.lightcopy.history.event.TaskEndReason;
import com.github.lightcopy.history.event.TaskInfo;

public class Task extends AbstractCodec<Task> {
  // task processing status
  public enum Status {
    UNKNOWN, RUNNING, GET_RESULT, FAILED, KILLED, SUCCESS
  }

  public static final String FIELD_TASK_ID = "taskId";
  public static final String FIELD_STAGE_ID = "stageId";
  public static final String FIELD_STAGE_ATTEMPT_ID = "stageAttemptId";
  public static final String FIELD_INDEX = "index";
  public static final String FIELD_ATTEMPT = "attempt";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_EXECUTOR_ID = "executorId";
  public static final String FIELD_HOST = "host";
  public static final String FIELD_LOCALITY = "locality";
  public static final String FIELD_SPECULATIVE = "speculative";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_ERR_DESC = "errorDescription";
  public static final String FIELD_ERR_DETAILS = "errorDetails";

  // unique identifier for task within application
  private long taskId;
  private int stageId;
  private int stageAttemptId;
  // task index within task set
  private int index;
  private int attempt;
  private long starttime;
  private long endtime;
  private String executorId;
  private String host;
  private String locality;
  private boolean speculative;
  private Status status;
  private long duration;
  private String errorDescription;
  private String errorDetails;

  public Task() {
    this.taskId = -1L;
    this.stageId = -1;
    this.stageAttemptId = -1;
    this.index = -1;
    this.attempt = -1;
    this.starttime = -1L;
    this.endtime = -1L;
    this.executorId = null;
    this.host = null;
    this.locality = null;
    this.speculative = false;
    this.status = Status.UNKNOWN;
    this.duration = -1L;
    this.errorDescription = null;
    this.errorDetails = null;
  }

  // == Getters ==

  public long getTaskId() {
    return taskId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getStageAttemptId() {
    return stageAttemptId;
  }

  public int getIndex() {
    return index;
  }

  public int getAttempt() {
    return attempt;
  }

  public long getStartTime() {
    return starttime;
  }

  public long getEndTime() {
    return endtime;
  }

  public String getExecutorId() {
    return executorId;
  }

  public String getHost() {
    return host;
  }

  public String getLocality() {
    return locality;
  }

  public boolean getSpeculative() {
    return speculative;
  }

  public Status getStatus() {
    return status;
  }

  public long getDuration() {
    return duration;
  }

  public String getErrorDescription() {
    return errorDescription;
  }

  public String getErrorDetails() {
    return errorDetails;
  }

  // == Setters ==

  public void setTaskId(long value) {
    this.taskId = value;
  }

  public void setStageId(int value) {
    this.stageId = value;
  }

  public void setStageAttemptId(int value) {
    this.stageAttemptId = value;
  }

  public void setIndex(int value) {
    this.index = value;
  }

  public void setAttempt(int value) {
    this.attempt = value;
  }

  public void setStartTime(long value) {
    this.starttime = value;
  }

  public void setEndTime(long value) {
    this.endtime = value;
  }

  public void setExecutorId(String value) {
    this.executorId = value;
  }

  public void setHost(String value) {
    this.host = value;
  }

  public void setLocality(String value) {
    this.locality = value;
  }

  public void setSpeculative(boolean value) {
    this.speculative = value;
  }

  public void setStatus(Status value) {
    this.status = value;
  }

  public void setDuration(long value) {
    this.duration = value;
  }

  public void setErrorDescription(String value) {
    this.errorDescription = value;
  }

  public void setErrorDetails(String value) {
    this.errorDetails = value;
  }

  /**
   * Update current task including status from task info.
   * @param info TaskInfo instance
   */
  public void update(TaskInfo info) {
    setTaskId(info.taskId);
    setIndex(info.index);
    setAttempt(info.attempt);
    setStartTime((info.launchTime <= 0) ? -1L : info.launchTime);
    setEndTime((info.finishTime <= 0) ? -1L : info.finishTime);
    setExecutorId(info.executorId);
    setHost(info.host);
    setLocality(info.locality);
    setSpeculative(info.speculative);
    // set task status
    if (info.finishTime != 0) {
      if (info.failed) {
        setStatus(Status.FAILED);
      } else if (info.killed) {
        setStatus(Status.KILLED);
      } else {
        setStatus(Status.SUCCESS);
      }
    } else {
      if (info.gettingResultTime != 0) {
        setStatus(Status.GET_RESULT);
      } else {
        setStatus(Status.RUNNING);
      }
    }
    // set duration if both time values are correctly set
    if (starttime >= 0 && endtime >= starttime) {
      setDuration(endtime - starttime);
    }
  }

  /**
   * Update current task with task end reason.
   * @param reason TaskEndReason instance
   */
  public void update(TaskEndReason reason) {
    // success reason will return empty string as description and null as details
    setErrorDescription(reason.getDescription());
    setErrorDetails(reason.getDetails());
  }

  // == Codec methods ==

  @Override
  public Task decode(BsonReader reader, DecoderContext decoderContext) {
    Task task = new Task();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_TASK_ID:
          task.setTaskId(reader.readInt64());
          break;
        case FIELD_STAGE_ID:
          task.setStageId(reader.readInt32());
          break;
        case FIELD_STAGE_ATTEMPT_ID:
          task.setStageAttemptId(reader.readInt32());
          break;
        case FIELD_INDEX:
          task.setIndex(reader.readInt32());
          break;
        case FIELD_ATTEMPT:
          task.setAttempt(reader.readInt32());
          break;
        case FIELD_STARTTIME:
          task.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          task.setEndTime(reader.readInt64());
          break;
        case FIELD_EXECUTOR_ID:
          task.setExecutorId(safeReadString(reader));
          break;
        case FIELD_HOST:
          task.setHost(safeReadString(reader));
          break;
        case FIELD_LOCALITY:
          task.setLocality(safeReadString(reader));
          break;
        case FIELD_SPECULATIVE:
          task.setSpeculative(reader.readBoolean());
          break;
        case FIELD_STATUS:
          task.setStatus(Status.valueOf(safeReadString(reader)));
          break;
        case FIELD_DURATION:
          task.setDuration(reader.readInt64());
          break;
        case FIELD_ERR_DESC:
          task.setErrorDescription(safeReadString(reader));
          break;
        case FIELD_ERR_DETAILS:
          task.setErrorDetails(safeReadString(reader));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return task;
  }

  @Override
  public Class<Task> getEncoderClass() {
    return Task.class;
  }

  @Override
  public void encode(BsonWriter writer, Task value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    writer.writeInt64(FIELD_TASK_ID, value.getTaskId());
    writer.writeInt32(FIELD_STAGE_ID, value.getStageId());
    writer.writeInt32(FIELD_STAGE_ATTEMPT_ID, value.getStageAttemptId());
    writer.writeInt32(FIELD_INDEX, value.getIndex());
    writer.writeInt32(FIELD_ATTEMPT, value.getAttempt());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    safeWriteString(writer, FIELD_EXECUTOR_ID, value.getExecutorId());
    safeWriteString(writer, FIELD_HOST, value.getHost());
    safeWriteString(writer, FIELD_LOCALITY, value.getLocality());
    writer.writeBoolean(FIELD_SPECULATIVE, value.getSpeculative());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_ERR_DESC, value.getErrorDescription());
    safeWriteString(writer, FIELD_ERR_DETAILS, value.getErrorDetails());
    writer.writeEndDocument();
  }
}
