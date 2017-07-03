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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Job;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
import com.github.lightcopy.history.model.Task;

/**
 * Class to keep constants for MongoDB, e.g. database names, collections, etc.
 * Provides utils to work with collections.
 */
public class Mongo {
  public static final String DATABASE = "history_server";
  public static final String APPLICATION_COLLECTION = "applications";
  public static final String ENVIRONMENT_COLLECTION = "environment";
  public static final String JOB_COLLECTION = "jobs";
  public static final String SQLEXECUTION_COLLECTION = "sqlexecution";
  public static final String STAGE_COLLECTION = "stages";
  public static final String TASK_COLLECTION = "tasks";

  /**
   * Get mongo collection for Application.
   * @param client Mongo client
   * @return collection for Application
   */
  public static MongoCollection<Application> applications(MongoClient client) {
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
   * Get mongo collection for Environment.
   * @param client Mongo client
   * @return collection for Environment
   */
  public static MongoCollection<Environment> environment(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(ENVIRONMENT_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new Environment());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(Environment.class);
  }

  /**
   * Get mongo collection for SQLExecution.
   * @param client Mongo client
   * @return collection for SQLExecution
   */
  public static MongoCollection<SQLExecution> sqlExecution(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(SQLEXECUTION_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new SQLExecution());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(SQLExecution.class);
  }

  /**
   * Get mongo collection for Task.
   * @param client Mongo client
   * @return collection for Task
   */
  public static MongoCollection<Task> tasks(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(TASK_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new Task());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(Task.class);
  }

  /**
   * Get mongo collection for Stage.
   * @param client Mongo client
   * @return collection for Stage
   */
  public static MongoCollection<Stage> stages(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(STAGE_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new Stage());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(Stage.class);
  }

  /**
   * Get mongo collection for Job.
   * @param client Mongo client
   * @return collection for Job
   */
  public static MongoCollection<Job> jobs(MongoClient client) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(JOB_COLLECTION);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new Job());
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(Job.class);
  }

  /**
   * Method to create unique ascending index for collection.
   * @param collection any Mongo collection
   * @param field field to index
   */
  public static void createUniqueIndex(MongoCollection<?> collection, String... fields) {
    IndexOptions indexOptions = new IndexOptions().unique(true);
    collection.createIndex(Indexes.ascending(fields), indexOptions);
  }

  /**
   * Build all indexes for collections.
   * @param client Mongo client
   */
  public static void buildIndexes(MongoClient client) {
    createUniqueIndex(applications(client), Application.FIELD_APP_ID);
    createUniqueIndex(environment(client), Environment.FIELD_APP_ID);
    createUniqueIndex(sqlExecution(client),
      SQLExecution.FIELD_APP_ID, SQLExecution.FIELD_EXECUTION_ID);
    // task table will contain several indexes, one to update tasks, another one to query by
    // job and stages
    createUniqueIndex(tasks(client), Task.FIELD_APP_ID, Task.FIELD_TASK_ID);
    createUniqueIndex(tasks(client), Task.FIELD_APP_ID, Task.FIELD_STAGE_ID,
      Task.FIELD_STAGE_ATTEMPT_ID, Task.FIELD_INDEX, Task.FIELD_ATTEMPT);
    // stages contain two indexes, one is for insertion and quick look up and another one to
    // search stages for job
    createUniqueIndex(stages(client), Stage.FIELD_APP_ID, Stage.FIELD_UNIQUE_STAGE_ID);
    createUniqueIndex(stages(client), Stage.FIELD_APP_ID, Stage.FIELD_JOB_ID,
      Stage.FIELD_STAGE_ID, Stage.FIELD_STAGE_ATTEMPT_ID);
    // jobs have only one index appId - jobId
    createUniqueIndex(jobs(client), Job.FIELD_APP_ID, Job.FIELD_JOB_ID);
  }

  /**
   * Clean up state (remove all data) based on provided app ids.
   * @param client Mongo client
   * @param appIds list of application ids to remove
   */
  public static void removeData(MongoClient client, List<String> appIds) {
    applications(client).deleteMany(Filters.all(Application.FIELD_APP_ID, appIds));
    environment(client).deleteMany(Filters.all(Environment.FIELD_APP_ID, appIds));
    sqlExecution(client).deleteMany(Filters.all(SQLExecution.FIELD_APP_ID, appIds));
    tasks(client).deleteMany(Filters.all(Task.FIELD_APP_ID, appIds));
    stages(client).deleteMany(Filters.all(Stage.FIELD_APP_ID, appIds));
    jobs(client).deleteMany(Filters.all(Job.FIELD_APP_ID, appIds));
  }

  /**
   * Clean up state (remove all data) based on provided app ids.
   * @param client Mongo client
   * @param appId application id to remove
   */
  public static void removeData(MongoClient client, String appId) {
    List<String> appIds = new ArrayList<String>();
    appIds.add(appId);
    removeData(client, appIds);
  }

  /** Smple upsert block to provide when updating single item in collection */
  public static interface UpsertBlock<T> {
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
   * If block.update() method returns null, upsert is ignored, and old document or null remains
   * unmodified.
   *
   * @param collection Mongo collection
   * @param filter filter to fetch one item
   * @param block block with upsert logic
   */
  public static <T> void findOneAndUpsert(
      MongoCollection<T> collection, Bson filter, UpsertBlock<T> block) {
    T item = collection.find(filter).first();
    T update = block.update(item);
    if (update != null) {
      UpdateOptions options = new UpdateOptions().upsert(true);
      UpdateResult res = collection.replaceOne(filter, update, options);
      if (!res.wasAcknowledged()) {
        throw new RuntimeException("Failed to upsert item " + update + ", original " + item);
      }
    }
  }

  /**
   * Find documents on a specific page.
   * @param collection mongo collection
   * @param filter optional filter, can be null
   * @param page page number, 1-based
   * @param pageSize page size
   * @param sortBy field to sort by
   * @param asc true if ascending sort, false otherwise
   * @return iterable with documents
   */
  public static <T> FindIterable<T> page(
      MongoCollection<T> collection,
      Bson filter,
      int page,
      int pageSize,
      String sortBy,
      boolean asc) {
    if (page <= 0 || page >= 100000) {
      throw new IllegalArgumentException("Invalid page " + page);
    }
    if (pageSize <= 0 || pageSize >= 100000) {
      throw new IllegalArgumentException("Invalid page size " + pageSize);
    }
    FindIterable<T> iter = (filter == null) ? collection.find() : collection.find(filter);
    // if sortBy field is empty - do not sort at all
    if (sortBy != null && !sortBy.isEmpty()) {
      Bson sortedBy = asc ? Sorts.ascending(sortBy) : Sorts.descending(sortBy);
      iter = iter.sort(sortedBy);
    }
    // calculate number of records to skip before requested page
    int skipRecords = (page - 1) * pageSize;
    return iter.skip(skipRecords).limit(pageSize);
  }
}
