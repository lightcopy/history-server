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

import java.util.ArrayList;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.github.lightcopy.history.Mongo;

/**
 * Class to represent SQL information for jobs.
 * Execution status does not failures, because query is only scheduled, if it is correct, and
 * is always completed, even if underlying jobs failed.
 */
public class SQLExecution extends AbstractCodec<SQLExecution> {
  // Processing status for event log instance
  public enum Status {
    NONE, RUNNING, COMPLETED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_EXECUTION_ID = "executionId";
  public static final String FIELD_DESCRIPTION = "description";
  public static final String FIELD_DETAILS = "details";
  public static final String FIELD_PHYSICAL_PLAN = "physicalPlan";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_JOB_IDS = "jobIds";

  private String appId;
  private int executionId;
  private String description;
  private String details;
  private String physicalPlan;
  private long starttime;
  private long endtime;
  private long duration;
  private Status status;
  private ArrayList<Integer> jobIds;

  public SQLExecution() {
    this.appId = null;
    this.executionId = -1;
    this.description = null;
    this.details = null;
    this.physicalPlan = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.NONE;
    this.jobIds = new ArrayList<Integer>();
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public int getExecutionId() {
    return executionId;
  }

  public String getDescription() {
    return description;
  }

  public String getDetails() {
    return details;
  }

  public String getPhysicalPlan() {
    return physicalPlan;
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

  public ArrayList<Integer> getJobIds() {
    return jobIds;
  }

  // == Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setExecutionId(int value) {
    this.executionId = value;
  }

  public void setDescription(String value) {
    this.description = value;
  }

  public void setDetails(String value) {
    this.details = value;
  }

  public void setPhysicalPlan(String value) {
    this.physicalPlan = value;
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

  public void setJobIds(ArrayList<Integer> value) {
    this.jobIds = value;
  }

  /** Add job id to the query */
  public void addJobId(int jobId) {
    this.jobIds.add(jobId);
  }

  // == Codec methods ==

  @Override
  public SQLExecution decode(BsonReader reader, DecoderContext decoderContext) {
    SQLExecution sql = new SQLExecution();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          sql.setAppId(safeReadString(reader));
          break;
        case FIELD_EXECUTION_ID:
          sql.setExecutionId(reader.readInt32());
          break;
        case FIELD_DESCRIPTION:
          sql.setDescription(safeReadString(reader));
          break;
        case FIELD_DETAILS:
          sql.setDetails(safeReadString(reader));
          break;
        case FIELD_PHYSICAL_PLAN:
          sql.setPhysicalPlan(safeReadString(reader));
          break;
        case FIELD_STARTTIME:
          sql.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          sql.setEndTime(reader.readInt64());
          break;
        case FIELD_DURATION:
          sql.setDuration(reader.readInt64());
          break;
        case FIELD_STATUS:
          sql.setStatus(Status.valueOf(safeReadString(reader)));
          break;
        case FIELD_JOB_IDS:
          sql.setJobIds(readList(reader, INT_ENCODER));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return sql;
  }

  @Override
  public Class<SQLExecution> getEncoderClass() {
    return SQLExecution.class;
  }

  @Override
  public void encode(BsonWriter writer, SQLExecution value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    writer.writeInt32(FIELD_EXECUTION_ID, value.getExecutionId());
    safeWriteString(writer, FIELD_DESCRIPTION, value.getDescription());
    safeWriteString(writer, FIELD_DETAILS, value.getDetails());
    safeWriteString(writer, FIELD_PHYSICAL_PLAN, value.getPhysicalPlan());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    writeList(writer, FIELD_JOB_IDS, value.getJobIds(), INT_ENCODER);
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static SQLExecution getOrCreate(MongoClient client, String appId, int executionId) {
    SQLExecution sql = Mongo.sqlExecution(client).find(
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_EXECUTION_ID, executionId)
      )).first();
    if (sql == null) {
      sql = new SQLExecution();
      sql.setAppId(appId);
      sql.setExecutionId(executionId);
    }
    sql.setMongoClient(client);
    return sql;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null || executionId < 0) return;
    Mongo.upsertOne(
      Mongo.sqlExecution(client),
      Filters.and(
        Filters.eq(FIELD_APP_ID, appId),
        Filters.eq(FIELD_EXECUTION_ID, executionId)
      ),
      this
    );
  }
}
