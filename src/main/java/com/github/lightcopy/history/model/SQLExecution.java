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

/**
 * Class to represent SQL information for jobs.
 */
public class SQLExecution extends AbstractCodec<SQLExecution> {
  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_EXECUTION_ID = "executionId";
  public static final String FIELD_DESCRIPTION = "description";
  public static final String FIELD_DETAILS = "details";
  public static final String FIELD_PHYSICAL_PLAN = "physicalPlan";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";

  private String appId;
  private int executionId;
  private String description;
  private String details;
  private String physicalPlan;
  private long starttime;
  private long endtime;

  public SQLExecution() {
    this.appId = null;
    this.executionId = -1;
    this.description = null;
    this.details = null;
    this.physicalPlan = null;
    this.starttime = -1L;
    this.endtime = -1L;
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
    writer.writeEndDocument();
  }
}
