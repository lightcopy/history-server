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
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;

public abstract class AbstractCodec<T> implements Codec<T> {
  /**
   * Method to write String safely bypassing null values.
   * @param writer bson writer to use
   * @param name key
   * @param value string value or null
   */
  public static void safeWriteString(BsonWriter writer, String name, String value) {
    if (value == null) {
      writer.writeNull(name);
    } else {
      writer.writeString(name, value);
    }
  }

  /**
   * Method to write String safely bypassing null values.
   * Writes value only.
   * @param writer bson writer
   * @param value string to write or null
   */
  public static void safeWriteString(BsonWriter writer, String value) {
    if (value == null) {
      writer.writeNull();
    } else {
      writer.writeString(value);
    }
  }

  /**
   * Read string safely either returning null or valid value.
   * @param reader bson reader
   * @return string or null
   */
  public static String safeReadString(BsonReader reader) {
    if (reader.getCurrentBsonType() == BsonType.NULL) {
      reader.readNull();
      return null;
    } else {
      return reader.readString();
    }
  }

  /** ReadItem for for deserializing collection */
  public static interface ReadItem<T> {
    T read(BsonReader reader);
  }

  /** WriteItem for serializing collection */
  public static interface WriteItem<T> {
    void write(BsonWriter writer, T value);
  }

  /** Internal class to provide both implementations for basic types */
  private static abstract class ReadWriteItem<T> implements ReadItem<T>, WriteItem<T> { }

  /** Encoder for String type */
  public static final ReadWriteItem<String> STRING_ITEM = new ReadWriteItem<String>() {
    @Override
    public String read(BsonReader reader) {
      return safeReadString(reader);
    }

    @Override
    public void write(BsonWriter writer, String value) {
      safeWriteString(writer, value);
    }
  };

  /** Encoder for Long type */
  public static final ReadWriteItem<Long> LONG_ITEM = new ReadWriteItem<Long>() {
    @Override
    public Long read(BsonReader reader) {
      return reader.readInt64();
    }

    @Override
    public void write(BsonWriter writer, Long value) {
      writer.writeInt64(value);
    }
  };

  /** Encoder for Integer type */
  public static final ReadWriteItem<Integer> INT_ITEM = new ReadWriteItem<Integer>() {
    @Override
    public Integer read(BsonReader reader) {
      return reader.readInt32();
    }

    @Override
    public void write(BsonWriter writer, Integer value) {
      writer.writeInt32(value);
    }
  };

  /**
   * Read list from bson document.
   * Requires deserialization block to parse T items.
   * @param reader bson reader
   * @param block deserialization block
   * @return list
   */
  public <T> ArrayList<T> readList(BsonReader reader, ReadItem<T> block) {
    ArrayList<T> list = new ArrayList<T>();
    reader.readStartArray();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      list.add(block.read(reader));
    }
    reader.readEndArray();
    return list;
  }

  /**
   * Write list as bson document.
   * Requires serialization block to parse T item.
   * @param writer bson writer
   * @param key document field key
   * @param value list value for that key
   * @param block serialization block
   */
  public <T> void writeList(BsonWriter writer, String key, ArrayList<T> value, WriteItem<T> block) {
    writer.writeStartArray(key);
    if (value != null) {
      for (T item : value) {
        block.write(writer, item);
      }
    }
    writer.writeEndArray();
  }

  /**
   * Read map from bson document.
   * We use array of tuples (key: "key", value: "value") to work around "." in key names.
   * @param reader bson reader
   * @return HashMap<String, T> instance
   */
  public <T> HashMap<String, T> readMap(BsonReader reader, ReadItem<T> block) {
    HashMap<String, T> map = new HashMap<String, T>();
    reader.readStartArray();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      String key = null;
      T value = null;
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        switch (reader.readName()) {
          case "key":
            key = safeReadString(reader);
            break;
          case "value":
            value = block.read(reader);
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
  public <T> void writeMap(
      BsonWriter writer, String key, HashMap<String, T> value, WriteItem<T> block) {
    writer.writeStartArray(key);
    if (value != null) {
      for (Map.Entry<String, T> entry : value.entrySet()) {
        writer.writeStartDocument();
        safeWriteString(writer, "key", entry.getKey());
        writer.writeName("value");
        block.write(writer, entry.getValue());
        writer.writeEndDocument();
      }
    }
    writer.writeEndArray();
  }
}
