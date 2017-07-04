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

public class Executor extends AbstractCodec<Executor> {
  public enum Status {
    UNKNOWN, ACTIVE, REMOVED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_EXECUTOR_ID = "executorId";
  public static final String FIELD_HOST = "host";
  public static final String FIELD_PORT = "port";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_DURATION = "duration";
  public static final String FIELD_STATUS = "status";

  private String appId;
  private String executorId;
  private String host;
  private int port;
  private long starttime;
  private long endtime;
  private long duration;
  private Status status;

  public Executor() {
    this.appId = null;
    this.executorId = null;
    this.host = null;
    this.port = -1;
    this.starttime = -1L;
    this.endtime = -1L;
    this.duration = -1L;
    this.status = Status.UNKNOWN;
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public String getExecutorId() {
    return executorId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
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

  // == Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setExecutorId(String value) {
    this.executorId = value;
  }

  public void setHost(String value) {
    this.host = value;
  }

  public void setPort(int value) {
    this.port = value;
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
        case FIELD_HOST:
          exc.setHost(safeReadString(reader));
          break;
        case FIELD_PORT:
          exc.setPort(reader.readInt32());
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
    safeWriteString(writer, FIELD_HOST, value.getHost());
    writer.writeInt32(FIELD_PORT, value.getPort());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    writer.writeInt64(FIELD_DURATION, value.getDuration());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    writer.writeEndDocument();
  }
}
