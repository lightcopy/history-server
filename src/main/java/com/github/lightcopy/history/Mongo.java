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

import java.util.ArrayList;
import java.util.List;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.EventLog;

/**
 * Class to keep constants for MongoDB, e.g. database names, collections, etc.
 * Provides utils to work with collections.
 */
public class Mongo {
  public static final String DATABASE = "history_server";
  public static final String EVENT_LOG_COLLECTION = "event_log";
  public static final String APPLICATION_COLLECTION = "applications";

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
   * Get mongo collection for Application based on client.
   * @param client Mongo client
   * @return collection for Application
   */
  public static MongoCollection<Application> applicationCollection(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(APPLICATION_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new Application());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(Application.class);
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

  /**
   * Clean up state (remove all data) based on provided app ids.
   * @param client Mongo client
   * @param logs event logs to remove
   */
  public static void removeData(MongoClient client, List<EventLog> logs) {
    List<String> appIds = new ArrayList<String>();
    for (EventLog log : logs) {
      appIds.add(log.getAppId());
    }
    Mongo.eventLogCollection(client).deleteMany(Filters.all(EventLog.FIELD_APP_ID, appIds));
    Mongo.applicationCollection(client).deleteMany(Filters.all(Application.FIELD_APP_ID, appIds));
  }

  /**
   * Clean up state (remove all data) based on provided app ids.
   * @param client Mongo client
   * @param log event log to remove
   */
  public static void removeData(MongoClient client, EventLog log) {
    List<EventLog> logs = new ArrayList<EventLog>();
    logs.add(log);
    removeData(client, logs);
  }

  /** Smple upsert block to provide when updating single item in collection */
  static interface UpsertBlock<T> {
    /**
     * Update provided item and return that update.
     * @param item found item or null if there is no filter match
     * @return updated item
     */
    T update(T item);
  }

  /**
   * Find document for provided filter in collection and apply update in block and upsert item in
   * collection. If original item exists, it will be replaced with new item; otherwise new item will
   * be inserted. This is similar to update, but allows us to work with Java classes directly rather
   * than constructing update.
   *
   * Client must ensure that filter returns only one record, otherwise upsert might update the
   * wrong document.
   *
   * @param collection Mongo collection
   * @param filter filter to fetch one item
   * @param block block with upsert logic
   */
  public static <T> void findOneAndUpsert(
      MongoCollection<T> collection, Bson filter, UpsertBlock<T> block) {
    T item = collection.find(filter).first();
    T update = block.update(item);
    UpdateOptions options = new UpdateOptions().upsert(true);
    UpdateResult res = collection.replaceOne(filter, update, options);
    if (!res.wasAcknowledged()) {
      throw new RuntimeException("Failed to upsert item " + update + ", original " + item);
    }
  }
}
