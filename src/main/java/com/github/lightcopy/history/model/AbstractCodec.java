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
   * @return HashMap<string, string> instance
   */
  public HashMap<String, String> readMap(BsonReader reader) {
    HashMap<String, String> map = new HashMap<String, String>();
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
  public void writeMap(BsonWriter writer, String key, HashMap<String, String> value) {
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
}
