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
import java.util.Collections;
import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.github.lightcopy.history.Mongo;

/**
 * Class to represent environment information for application.
 */
public class Environment extends AbstractCodec<Environment> {
  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_JVM_INFO = "jvmInformation";
  public static final String FIELD_SPARK_PROPS = "sparkProperties";
  public static final String FIELD_SYSTEM_PROPS = "systemProperties";
  public static final String FIELD_CLASSPATH_ENT = "classpathEntries";

  /**
   * Internal entry format as key-value pair for environment properties.
   * Similar to tuple in both equality and comparison.
   */
  public static class Entry implements Comparable<Entry> {
    private String name;
    private String value;

    public Entry() {
      /* no-op */
    }

    public Entry(String name, String value) {
      this.name = name;
      this.value = value;
    }

    /** Get name for this entry */
    public String getName() {
      return name;
    }

    /** Get value for this entry */
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Entry)) return false;
      Entry that = (Entry) obj;
      return this.name.equals(that.name) && this.value.equals(that.value);
    }

    @Override
    public int compareTo(Entry other) {
      int res = this.name.compareTo(other.name);
      if (res != 0) return res;
      return this.value.compareTo(other.value);
    }

    @Override
    public String toString() {
      return "(" + name + " -> " + value + ")";
    }
  }

  // read block for Mongo serde
  public static final ReadEncoder<Entry> READ_ENCODER = new ReadEncoder<Entry>() {
    @Override
    public Entry read(BsonReader reader) {
      String name = null;
      String value = null;
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        switch (reader.readName()) {
          case "name":
            name = safeReadString(reader);
            break;
          case "value":
            value = safeReadString(reader);
            break;
          default:
            break;
        }
      }
      reader.readEndDocument();
      return new Entry(name, value);
    }
  };

  // write block for Mongo serde
  public static final WriteEncoder<Entry> WRITE_ENCODER = new WriteEncoder<Entry>() {
    @Override
    public void write(BsonWriter writer, Entry entry) {
      writer.writeStartDocument();
      safeWriteString(writer, "name", entry.getName());
      safeWriteString(writer, "value", entry.getValue());
      writer.writeEndDocument();
    }
  };

  private String appId;
  // environment information
  private ArrayList<Entry> jvmInformation;
  private ArrayList<Entry> sparkProperties;
  private ArrayList<Entry> systemProperties;
  private ArrayList<Entry> classpathEntries;

  public Environment() {
    this.appId = null;
    this.jvmInformation = new ArrayList<Entry>();
    this.sparkProperties = new ArrayList<Entry>();
    this.systemProperties = new ArrayList<Entry>();
    this.classpathEntries = new ArrayList<Entry>();
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public ArrayList<Entry> getJvmInformation() {
    return jvmInformation;
  }

  public ArrayList<Entry> getSparkProperties() {
    return sparkProperties;
  }

  public ArrayList<Entry> getSystemProperties() {
    return systemProperties;
  }

  public ArrayList<Entry> getClasspathEntries() {
    return classpathEntries;
  }

  // == Setters ==

  public void setAppId(String appId) {
    this.appId = appId;
  }

  /** Convert map into list of entries, sorted in ascending order */
  private ArrayList<Entry> mapToSortedList(Map<String, String> map) {
    ArrayList<Entry> list = new ArrayList<Entry>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      list.add(new Entry(entry.getKey(), entry.getValue()));
    }
    // sort in ascending order
    Collections.sort(list);
    return list;
  }

  public void setJvmInformation(Map<String, String> info) {
    this.jvmInformation = mapToSortedList(info);
  }

  public void setSparkProperties(Map<String, String> props) {
    this.sparkProperties = mapToSortedList(props);
  }

  public void setSystemProperties(Map<String, String> props) {
    this.systemProperties = mapToSortedList(props);
  }

  public void setClasspathEntries(Map<String, String> entries) {
    this.classpathEntries = mapToSortedList(entries);
  }

  public void setJvmInformation(ArrayList<Entry> info) {
    this.jvmInformation = info;
  }

  public void setSparkProperties(ArrayList<Entry> props) {
    this.sparkProperties = props;
  }

  public void setSystemProperties(ArrayList<Entry> props) {
    this.systemProperties = props;
  }

  public void setClasspathEntries(ArrayList<Entry> entries) {
    this.classpathEntries = entries;
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
          env.setJvmInformation(readList(reader, READ_ENCODER));
          break;
        case FIELD_SPARK_PROPS:
          env.setSparkProperties(readList(reader, READ_ENCODER));
          break;
        case FIELD_SYSTEM_PROPS:
          env.setSystemProperties(readList(reader, READ_ENCODER));
          break;
        case FIELD_CLASSPATH_ENT:
          env.setClasspathEntries(readList(reader, READ_ENCODER));
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
    writeList(writer, FIELD_JVM_INFO, value.getJvmInformation(), WRITE_ENCODER);
    // write spark properties
    writeList(writer, FIELD_SPARK_PROPS, value.getSparkProperties(), WRITE_ENCODER);
    // write system properties
    writeList(writer, FIELD_SYSTEM_PROPS, value.getSystemProperties(), WRITE_ENCODER);
    // write classpath entries
    writeList(writer, FIELD_CLASSPATH_ENT, value.getClasspathEntries(), WRITE_ENCODER);
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static Environment getOrCreate(MongoClient client, String appId) {
    Environment env = Mongo.environment(client).find(Filters.eq(FIELD_APP_ID, appId)).first();
    if (env == null) {
      env = new Environment();
      env.setAppId(appId);
    }
    env.setMongoClient(client);
    return env;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null) return;
    Mongo.upsertOne(
      Mongo.environment(client),
      Filters.eq(FIELD_APP_ID, appId),
      this
    );
  }
}
