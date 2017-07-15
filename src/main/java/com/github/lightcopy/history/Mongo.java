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

import org.bson.codecs.Codec;
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

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.ApplicationSummary;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Executor;
import com.github.lightcopy.history.model.Job;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
import com.github.lightcopy.history.model.StageSummary;
import com.github.lightcopy.history.model.Task;

/**
 * Class to keep constants for MongoDB, e.g. database names, collections, etc.
 * Provides utils to work with collections.
 */
public class Mongo {
  public static final String DATABASE = "history_server";
  public static final String APPLICATION_COLLECTION = "applications";
  public static final String APP_SUMMARY_COLLECTION = "application_summary";
  public static final String ENVIRONMENT_COLLECTION = "environment";
  public static final String JOB_COLLECTION = "jobs";
  public static final String SQLEXECUTION_COLLECTION = "sqlexecution";
  public static final String STAGE_COLLECTION = "stages";
  public static final String STAGE_SUMMARY_COLLECTION = "stage_summary";
  public static final String TASK_COLLECTION = "tasks";
  public static final String EXECUTOR_COLLECTION = "executors";


  /** Generic get collection method with conversion codec */
  private static <T> MongoCollection<T> getCollection(
      MongoClient client, String collectionName, Class<T> clazz, Codec<T> codec) {
    MongoCollection<?> collection = client.getDatabase(DATABASE)
      .getCollection(collectionName);
    // extract codec registries to add new support
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(codec);
    return collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(clazz);
  }

  // == Encoders ==
  private static final Application APP_CODEC = new Application();
  private static final ApplicationSummary APP_SUM_CODEC = new ApplicationSummary();
  private static final Environment ENV_CODEC = new Environment();
  private static final SQLExecution SQL_CODEC = new SQLExecution();
  private static final Task TASK_CODEC = new Task();
  private static final Stage STAGE_CODEC = new Stage();
  private static final StageSummary STAGE_SUMMARY_CODEC = new StageSummary();
  private static final Job JOB_CODEC = new Job();
  private static final Executor EXC_CODEC = new Executor();

  /**
   * Get mongo collection for Application.
   * @param client Mongo client
   * @return collection for Application
   */
  public static MongoCollection<Application> applications(MongoClient client) {
    return getCollection(client, APPLICATION_COLLECTION, Application.class, APP_CODEC);
  }

  /**
   * Get mongo collection for ApplicationSummary.
   * @param client Mongo client
   * @return collection for ApplicationSummary
   */
  public static MongoCollection<ApplicationSummary> appSummary(MongoClient client) {
    return getCollection(client, APP_SUMMARY_COLLECTION, ApplicationSummary.class, APP_SUM_CODEC);
  }

  /**
   * Get mongo collection for Environment.
   * @param client Mongo client
   * @return collection for Environment
   */
  public static MongoCollection<Environment> environment(MongoClient client) {
    return getCollection(client, ENVIRONMENT_COLLECTION, Environment.class, ENV_CODEC);
  }

  /**
   * Get mongo collection for SQLExecution.
   * @param client Mongo client
   * @return collection for SQLExecution
   */
  public static MongoCollection<SQLExecution> sqlExecution(MongoClient client) {
    return getCollection(client, SQLEXECUTION_COLLECTION, SQLExecution.class, SQL_CODEC);
  }

  /**
   * Get mongo collection for Task.
   * @param client Mongo client
   * @return collection for Task
   */
  public static MongoCollection<Task> tasks(MongoClient client) {
    return getCollection(client, TASK_COLLECTION, Task.class, TASK_CODEC);
  }

  /**
   * Get mongo collection for Stage.
   * @param client Mongo client
   * @return collection for Stage
   */
  public static MongoCollection<Stage> stages(MongoClient client) {
    return getCollection(client, STAGE_COLLECTION, Stage.class, STAGE_CODEC);
  }

  /**
   * Get mongo collection for StageSummary.
   * @param client Mongo client
   * @return collection for StageSummary
   */
  public static MongoCollection<StageSummary> stageSummary(MongoClient client) {
    return getCollection(client, STAGE_SUMMARY_COLLECTION, StageSummary.class, STAGE_SUMMARY_CODEC);
  }

  /**
   * Get mongo collection for Job.
   * @param client Mongo client
   * @return collection for Job
   */
  public static MongoCollection<Job> jobs(MongoClient client) {
    return getCollection(client, JOB_COLLECTION, Job.class, JOB_CODEC);
  }

  /**
   * Get mongo collection for Executor.
   * @param client Mongo client
   * @return collection for Executor
   */
  public static MongoCollection<Executor> executors(MongoClient client) {
    return getCollection(client, EXECUTOR_COLLECTION, Executor.class, EXC_CODEC);
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
    createUniqueIndex(appSummary(client), ApplicationSummary.FIELD_APP_ID);
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
    createUniqueIndex(stages(client), Stage.FIELD_APP_ID,
      Stage.FIELD_STAGE_ID, Stage.FIELD_STAGE_ATTEMPT_ID);
    createUniqueIndex(stages(client), Stage.FIELD_APP_ID,
      Stage.FIELD_JOB_ID, Stage.FIELD_STAGE_ID, Stage.FIELD_STAGE_ATTEMPT_ID);
    // stage summary index (similar to stages)
    createUniqueIndex(stageSummary(client), StageSummary.FIELD_APP_ID,
      StageSummary.FIELD_STAGE_ID, StageSummary.FIELD_STAGE_ATTEMPT_ID);
    // jobs have only one index appId - jobId
    createUniqueIndex(jobs(client), Job.FIELD_APP_ID, Job.FIELD_JOB_ID);
    // executors have only one index appId - executorId
    createUniqueIndex(executors(client), Executor.FIELD_APP_ID, Executor.FIELD_EXECUTOR_ID);
  }

  /**
   * Clean up state (remove all data) based on provided app ids.
   * @param client Mongo client
   * @param appIds list of application ids to remove
   */
  public static void removeData(MongoClient client, List<String> appIds) {
    applications(client).deleteMany(Filters.all(Application.FIELD_APP_ID, appIds));
    appSummary(client).deleteMany(Filters.all(ApplicationSummary.FIELD_APP_ID, appIds));
    environment(client).deleteMany(Filters.all(Environment.FIELD_APP_ID, appIds));
    sqlExecution(client).deleteMany(Filters.all(SQLExecution.FIELD_APP_ID, appIds));
    tasks(client).deleteMany(Filters.all(Task.FIELD_APP_ID, appIds));
    stages(client).deleteMany(Filters.all(Stage.FIELD_APP_ID, appIds));
    stageSummary(client).deleteMany(Filters.all(StageSummary.FIELD_APP_ID, appIds));
    jobs(client).deleteMany(Filters.all(Job.FIELD_APP_ID, appIds));
    executors(client).deleteMany(Filters.all(Executor.FIELD_APP_ID, appIds));
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

  /**
   * Atomic upsert for single record.
   *
   * If document is not found method would insert replacement. Client must ensure that all ids
   * confirm to the index structures created for collection.
   * If document was found, it will be replaced with provided replacement.
   *
   * @param collection collection to insert into
   * @param filter filter to use, mostly by ids
   * @param replacement document to replace
   */
  public static <T> void upsertOne(MongoCollection<T> collection, Bson filter, T replacement) {
    UpdateOptions options = new UpdateOptions().upsert(true);
    collection.replaceOne(filter, replacement, options);
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
