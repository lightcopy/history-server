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
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Class to represent environment information for application.
 */
public class Environment extends AbstractCodec<Environment> {
  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_JVM_INFO = "jvmInformation";
  public static final String FIELD_SPARK_PROPS = "sparkProperties";
  public static final String FIELD_SYSTEM_PROPS = "systemProperties";
  public static final String FIELD_CLASSPATH_ENT = "classpathEntries";

  private String appId;
  // environment information
  private HashMap<String, String> jvmInformation;
  private HashMap<String, String> sparkProperties;
  private HashMap<String, String> systemProperties;
  private HashMap<String, String> classpathEntries;

  public Environment() {
    this.jvmInformation = new HashMap<String, String>();
    this.sparkProperties = new HashMap<String, String>();
    this.systemProperties = new HashMap<String, String>();
    this.classpathEntries = new HashMap<String, String>();
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public HashMap<String, String> getJvmInformation() {
    return jvmInformation;
  }

  public HashMap<String, String> getSparkProperties() {
    return sparkProperties;
  }

  public HashMap<String, String> getSystemProperties() {
    return systemProperties;
  }

  public HashMap<String, String> getClasspathEntries() {
    return classpathEntries;
  }

  // == Setters ==

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public void setJvmInformation(Map<String, String> info) {
    this.jvmInformation = new HashMap<String, String>(info);
  }

  public void setSparkProperties(Map<String, String> props) {
    this.sparkProperties = new HashMap<String, String>(props);
  }

  public void setSystemProperties(Map<String, String> props) {
    this.systemProperties = new HashMap<String, String>(props);
  }

  public void setClasspathEntries(Map<String, String> entries) {
    this.classpathEntries = new HashMap<String, String>(entries);
  }

  // == Codec methods ==

  @Override
  public Environment decode(BsonReader reader, DecoderContext decoderContext) {
    Environment env = new Environment();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          env.setAppId(safeReadString(reader));
          break;
        case FIELD_JVM_INFO:
          env.setJvmInformation(readMap(reader));
          break;
        case FIELD_SPARK_PROPS:
          env.setSparkProperties(readMap(reader));
          break;
        case FIELD_SYSTEM_PROPS:
          env.setSystemProperties(readMap(reader));
          break;
        case FIELD_CLASSPATH_ENT:
          env.setClasspathEntries(readMap(reader));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return env;
  }

  @Override
  public Class<Environment> getEncoderClass() {
    return Environment.class;
  }

  @Override
  public void encode(BsonWriter writer, Environment value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
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
