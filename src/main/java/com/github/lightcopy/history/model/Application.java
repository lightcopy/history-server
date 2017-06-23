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

import java.util.HashMap;
import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Class to represent Spark application and its metadata.
 */
public class Application implements Codec<Application> {
  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_APP_NAME = "appName";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_USER = "user";
  public static final String FIELD_JVM_INFO = "jvmInformation";
  public static final String FIELD_SPARK_PROPS = "sparkProperties";
  public static final String FIELD_SYSTEM_PROPS = "systemProperties";
  public static final String FIELD_CLASSPATH_ENT = "classpathEntries";

  private String name;
  private String id;
  private long starttime;
  private long endtime;
  private String user;
  // environment information
  private Map<String, String> jvmInformation;
  private Map<String, String> sparkProperties;
  private Map<String, String> systemProperties;
  private Map<String, String> classpathEntries;

  // default constructor for encode/decode
  public Application() {
    this.name = null;
    this.id = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.user = null;
    this.jvmInformation = new HashMap<String, String>();
    this.sparkProperties = new HashMap<String, String>();
    this.systemProperties = new HashMap<String, String>();
    this.classpathEntries = new HashMap<String, String>();
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

  public Map<String, String> getJvmInformation() {
    return jvmInformation;
  }

  public Map<String, String> getSparkProperties() {
    return sparkProperties;
  }

  public Map<String, String> getSystemProperties() {
    return systemProperties;
  }

  public Map<String, String> getClasspathEntries() {
    return classpathEntries;
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

  public void setJvmInformation(Map<String, String> info) {
    this.jvmInformation = info;
  }

  public void setSparkProperties(Map<String, String> props) {
    this.sparkProperties = props;
  }

  public void setSystemProperties(Map<String, String> props) {
    this.systemProperties = props;
  }

  public void setClasspathEntries(Map<String, String> entries) {
    this.classpathEntries = entries;
  }

  // == Codec methods ==

  /**
   * Method to write String safely bypassing null values.
   * @param writer bson writer to use
   * @param name key
   * @param value string value or null
   */
  public void safeWriteString(BsonWriter writer, String name, String value) {
    if (value == null) {
      writer.writeNull(name);
    } else {
      writer.writeString(name, value);
    }
  }

  /**
   * Read string safely either returning null or valid value.
   * @param reader bson reader
   * @return string or null
   */
  public String safeReadString(BsonReader reader) {
    if (reader.getCurrentBsonType() == BsonType.NULL) {
      reader.readNull();
      return null;
    } else {
      return reader.readString();
    }
  }

  /**
   * Read map from bson document.
   * We use array of tuples (key: "key", value: "value") to work around "." in key names.
   * @param reader bson reader
   * @return Map<string, string> instance
   */
  private Map<String, String> readMap(BsonReader reader) {
    Map<String, String> map = new HashMap<String, String>();
    reader.readStartArray();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      String key = null;
      String value = null;
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        switch (reader.readName()) {
          case "key":
            key = safeReadString(reader);
            break;
          case "value":
            value = safeReadString(reader);
            break;
          default:
            break;
        }
      }
      map.put(key, value);
      reader.readEndDocument();
    }
    reader.readEndArray();
    return map;
  }

  /**
   * Write map as bson document.
   * We use array of tuples (key: "key", value: "value") to work around "." in key names.
   * @param writer bson writer
   * @param key document field name
   * @param value map value for that key
   */
  private void writeMap(BsonWriter writer, String key, Map<String, String> value) {
    writer.writeStartArray(key);
    if (value != null) {
      for (Map.Entry<String, String> entry : value.entrySet()) {
        writer.writeStartDocument();
        safeWriteString(writer, "key", entry.getKey());
        safeWriteString(writer, "value", entry.getValue());
        writer.writeEndDocument();
      }
    }
    writer.writeEndArray();
  }

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
        case FIELD_JVM_INFO:
          app.setJvmInformation(readMap(reader));
          break;
        case FIELD_SPARK_PROPS:
          app.setSparkProperties(readMap(reader));
          break;
        case FIELD_SYSTEM_PROPS:
          app.setSystemProperties(readMap(reader));
          break;
        case FIELD_CLASSPATH_ENT:
          app.setClasspathEntries(readMap(reader));
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
    // write jvm information as nested document
    writeMap(writer, FIELD_JVM_INFO, value.getJvmInformation());
    // write spark properties
    writeMap(writer, FIELD_SPARK_PROPS, value.getSparkProperties());
    // write system properties
    writeMap(writer, FIELD_SYSTEM_PROPS, value.getSystemProperties());
    // write classpath entries
    writeMap(writer, FIELD_CLASSPATH_ENT, value.getClasspathEntries());
    writer.writeEndDocument();
  }
}
