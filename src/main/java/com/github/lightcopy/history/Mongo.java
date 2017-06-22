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

package com.github.lightcopy.history;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;

/**
 * Class to keep constants for MongoDB, e.g. database names, collections, etc.
 * Provides utils to work with collections.
 */
public class Mongo {
  public static final String DATABASE = "history_server";
  public static final String EVENT_LOG_COLLECTION = "event_log";

  /**
   * Get mongo collection for EventLog based on client.
   * @param client Mongo client
   * @return collection for EventLog
   */
  public static MongoCollection<EventLog> eventLogCollection(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(EVENT_LOG_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new EventLog());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(EventLog.class);
  }

  /**
   * Method to create unique ascending index for collection.
   * @param collection any Mongo collection
   * @param field field to index
   */
  public static void createUniqueIndex(MongoCollection<?> collection, String field) {
    IndexOptions indexOptions = new IndexOptions().unique(true);
    collection.createIndex(Indexes.ascending(field), indexOptions);
  }
}
