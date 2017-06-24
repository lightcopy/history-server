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
 * Class to represent Spark application and its metadata.
 */
public class Application extends AbstractCodec<Application> {
  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_APP_NAME = "appName";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_USER = "user";

  private String name;
  private String id;
  private long starttime;
  private long endtime;
  private String user;

  // default constructor for encode/decode
  public Application() {
    this.name = null;
    this.id = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.user = null;
  }

  // == getters ==

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public long getStartTime() {
    return starttime;
  }

  public long getEndTime() {
    return endtime;
  }

  public String getUser() {
    return user;
  }

  // == setters ==

  public void setName(String name) {
    this.name = name;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setStartTime(long timestamp) {
    this.starttime = timestamp;
  }

  public void setEndTime(long timestamp) {
    this.endtime = timestamp;
  }

  public void setUser(String user) {
    this.user = user;
  }

  // == Codec methods ==

  @Override
  public Application decode(BsonReader reader, DecoderContext decoderContext) {
    Application app = new Application();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          app.setId(safeReadString(reader));
          break;
        case FIELD_APP_NAME:
          app.setName(safeReadString(reader));
          break;
        case FIELD_STARTTIME:
          app.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          app.setEndTime(reader.readInt64());
          break;
        case FIELD_USER:
          app.setUser(safeReadString(reader));
          break;
        default:
          // ignore any other fields, e.g. object id
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return app;
  }

  @Override
  public Class<Application> getEncoderClass() {
    return Application.class;
  }

  @Override
  public void encode(BsonWriter writer, Application value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getId());
    safeWriteString(writer, FIELD_APP_NAME, value.getName());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    safeWriteString(writer, FIELD_USER, value.getUser());
    writer.writeEndDocument();
  }
}
