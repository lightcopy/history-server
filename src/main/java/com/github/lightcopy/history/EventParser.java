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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

import com.github.lightcopy.history.event.Event;
import com.github.lightcopy.history.event.SparkListenerApplicationStart;
import com.github.lightcopy.history.event.SparkListenerApplicationEnd;
import com.github.lightcopy.history.event.SparkListenerEnvironmentUpdate;
import com.github.lightcopy.history.event.SparkListenerSQLExecutionStart;
import com.github.lightcopy.history.event.SparkListenerSQLExecutionEnd;
import com.github.lightcopy.history.event.SparkListenerStageCompleted;
import com.github.lightcopy.history.event.SparkListenerStageSubmitted;
import com.github.lightcopy.history.event.SparkListenerTaskStart;
import com.github.lightcopy.history.event.SparkListenerTaskEnd;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Metrics;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
import com.github.lightcopy.history.model.StageAggregateTracker;
import com.github.lightcopy.history.model.Task;

/**
 * Parser for Spark listener events.
 * Performs aggregation for metrics and keeps aggregation state, and is initialized per
 * application log.
 * Class is not thread-safe and should be created per processing thread.
 */
public class EventParser {
  private static final Logger LOG = LoggerFactory.getLogger(EventParser.class);
  private static Gson gson = new Gson();

  private StageAggregateTracker stageAgg;

  public EventParser() {
    this.stageAgg = new StageAggregateTracker();
  }

  /**
   * Parse application logs from file.
   * @param fs file system
   * @param client Mongo client
   * @param app application to parse (has partial data related to fs file)
   */
  public void parseApplicationLog(FileSystem fs, MongoClient client, Application app)
      throws EventProcessException {
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(app.getPath()));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String json;
      while ((json = reader.readLine()) != null) {
        Event event = gson.fromJson(json, Event.class);
        if (event.getEventName() != null) {
          parseJsonEvent(app.getAppId(), event, json, client);
        } else {
          LOG.warn("Drop event {} for app {}", json, app.getAppId());
        }
      }
    } catch (Exception err) {
      throw new EventProcessException(err.getMessage(), err);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ioe) {
          // no-op
        }
      }
    }
  }

  /** Parse individual event from json string */
  private void parseJsonEvent(String appId, Event event, String json, MongoClient client) {
    switch (event.getEventName()) {
      case "SparkListenerApplicationStart":
        processEvent(client, appId, gson.fromJson(json, SparkListenerApplicationStart.class));
        break;
      case "SparkListenerApplicationEnd":
        processEvent(client, appId, gson.fromJson(json, SparkListenerApplicationEnd.class));
        break;
      case "SparkListenerEnvironmentUpdate":
        processEvent(client, appId, gson.fromJson(json, SparkListenerEnvironmentUpdate.class));
        break;
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
        processEvent(client, appId, gson.fromJson(json, SparkListenerSQLExecutionStart.class));
        break;
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
        processEvent(client, appId, gson.fromJson(json, SparkListenerSQLExecutionEnd.class));
        break;
      case "SparkListenerTaskStart":
        processEvent(client, appId, gson.fromJson(json, SparkListenerTaskStart.class));
        break;
      case "SparkListenerTaskEnd":
        processEvent(client, appId, gson.fromJson(json, SparkListenerTaskEnd.class));
        break;
      case "SparkListenerStageSubmitted":
        processEvent(client, appId, gson.fromJson(json, SparkListenerStageSubmitted.class));
        break;
      case "SparkListenerStageCompleted":
        processEvent(client, appId, gson.fromJson(json, SparkListenerStageCompleted.class));
        break;
      default:
        LOG.warn("Unrecongnized event {} ", event);
        break;
    }
  }

  // == Processing methods for listener events ==

  // == SparkListenerApplicationStart ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerApplicationStart event) {
    // check that app id is the same as event log id
    if (!appId.equals(event.appId)) {
      throw new RuntimeException("App ID mismatch: " + appId + " != " + event.appId);
    }

    Mongo.findOneAndUpsert(
      Mongo.applications(client),
      Filters.eq(Application.FIELD_APP_ID, appId),
      new Mongo.UpsertBlock<Application>() {
        @Override
        public Application update(Application obj) {
          if (obj == null) {
            obj = new Application();
            // update appId, because it is new application
            obj.setAppId(appId);
          }
          // update application based on event
          obj.setAppName(event.appName);
          obj.setStartTime(event.timestamp);
          obj.setUser(event.user);
          obj.setAppStatus(Application.AppStatus.IN_PROGRESS);
          return obj;
        }
      }
    );
  }

  // == SparkListenerApplicationEnd ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerApplicationEnd event) {
    Mongo.findOneAndUpsert(
      Mongo.applications(client),
      Filters.eq(Application.FIELD_APP_ID, appId),
      new Mongo.UpsertBlock<Application>() {
        @Override
        public Application update(Application obj) {
          if (obj == null) {
            // this covers test when application start event is missing
            obj = new Application();
            obj.setAppId(appId);
          }
          obj.setEndTime(event.timestamp);
          obj.setAppStatus(Application.AppStatus.FINISHED);
          return obj;
        }
      }
    );
  }

  // == SparkListenerEnvironmentUpdate ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerEnvironmentUpdate event) {
    Mongo.findOneAndUpsert(
      Mongo.environment(client),
      Filters.eq(Environment.FIELD_APP_ID, appId),
      new Mongo.UpsertBlock<Environment>() {
        @Override
        public Environment update(Environment obj) {
          if (obj == null) {
            // this is done because environment update comes before application start event
            obj = new Environment();
            obj.setAppId(appId);
          }
          obj.setJvmInformation(event.jvmInformation);
          obj.setSparkProperties(event.sparkProperties);
          obj.setSystemProperties(event.systemProperties);
          obj.setClasspathEntries(event.classpathEntries);
          return obj;
        }
      }
    );
  }

  // == SparkListenerSQLExecutionStart ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerSQLExecutionStart event) {
    Mongo.findOneAndUpsert(
      Mongo.sqlExecution(client),
      Filters.and(
        Filters.eq(SQLExecution.FIELD_APP_ID, appId),
        Filters.eq(SQLExecution.FIELD_EXECUTION_ID, event.executionId)
      ),
      new Mongo.UpsertBlock<SQLExecution>() {
        @Override
        public SQLExecution update(SQLExecution obj) {
          if (obj == null) {
            obj = new SQLExecution();
            obj.setAppId(appId);
            obj.setExecutionId(event.executionId);
          }
          obj.setDescription(event.description);
          obj.setDetails(event.details);
          obj.setPhysicalPlan(event.physicalPlanDescription);
          obj.setStartTime(event.time);
          obj.updateDuration();
          obj.setStatus(SQLExecution.Status.RUNNING);
          return obj;
        }
      }
    );
  }

  // == SparkListenerSQLExecutionEnd ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerSQLExecutionEnd event) {
    Mongo.findOneAndUpsert(
      Mongo.sqlExecution(client),
      Filters.and(
        Filters.eq(SQLExecution.FIELD_APP_ID, appId),
        Filters.eq(SQLExecution.FIELD_EXECUTION_ID, event.executionId)
      ),
      new Mongo.UpsertBlock<SQLExecution>() {
        @Override
        public SQLExecution update(SQLExecution obj) {
          if (obj == null) {
            obj = new SQLExecution();
            obj.setAppId(appId);
            obj.setExecutionId(event.executionId);
          }
          obj.setEndTime(event.time);
          obj.updateDuration();
          obj.setStatus(SQLExecution.Status.COMPLETED);
          return obj;
        }
      }
    );
  }

  // == SparkListenerTaskStart ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerTaskStart event) {
    Mongo.findOneAndUpsert(
      Mongo.tasks(client),
      Filters.and(
        Filters.eq(Task.FIELD_APP_ID, appId),
        Filters.eq(Task.FIELD_TASK_ID, event.taskInfo.taskId)
      ),
      new Mongo.UpsertBlock<Task>() {
        @Override
        public Task update(Task obj) {
          // we never receive task for the same appId-taskId combination
          if (obj != null) {
            // TODO: consider logging such events and continue
            throw new IllegalStateException("Duplicate task (" + appId + ", taskId=" +
              event.taskInfo.taskId + ")");
          }
          obj = new Task();
          obj.setAppId(appId);
          obj.setStageId(event.stageId);
          obj.setStageAttemptId(event.stageAttemptId);
          obj.update(event.taskInfo);
          return obj;
        }
      }
    );
    // increment number of active tasks per stage
    stageAgg.incActiveTasks(event.stageId, event.stageAttemptId);
  }

  // == SparkListenerTaskEnd ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerTaskEnd event) {
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. For now we allow processing of task, it will be assigned to stage -1
    // which does not exist and we never query by negative attempt
    Mongo.findOneAndUpsert(
      Mongo.tasks(client),
      Filters.and(
        Filters.eq(Task.FIELD_APP_ID, appId),
        Filters.eq(Task.FIELD_TASK_ID, event.taskInfo.taskId)
      ),
      new Mongo.UpsertBlock<Task>() {
        @Override
        public Task update(Task obj) {
          // task end should always be received after task start
          if (obj == null) {
            // TODO: consider logging such events and continue
            throw new IllegalStateException("Task end received before task start (" + appId +
              ", taskId=" + event.taskInfo.taskId + ")");
          }
          obj.setStageId(event.stageId);
          obj.setStageAttemptId(event.stageAttemptId);
          obj.update(event.taskInfo);
          obj.update(event.taskEndReason);
          obj.update(event.taskMetrics);
          return obj;
        }
      }
    );

    // update stage info with current metrics and stats snapshot
    stageAgg.decActiveTasks(event.stageId, event.stageAttemptId);
    if (event.taskEndReason.isSuccess()) {
      stageAgg.incCompletedTasks(event.stageId, event.stageAttemptId);
    } else {
      stageAgg.incFailedTasks(event.stageId, event.stageAttemptId);
    }
    Metrics update = new Metrics();
    update.set(event.taskMetrics);
    stageAgg.updateMetrics(event.stageId, event.stageAttemptId, update);
    // we also update stage with partial counts and metrics for cases when stage never completes
    // if stage complete event is received stage will updated with final metrics anyway.
    final int activeTasks = stageAgg.getActiveTasks(event.stageId, event.stageAttemptId);
    final int completedTasks = stageAgg.getCompletedTasks(event.stageId, event.stageAttemptId);
    final int failedTasks = stageAgg.getFailedTasks(event.stageId, event.stageAttemptId);
    final Metrics metrics = stageAgg.getMetrics(event.stageId, event.stageAttemptId);

    Mongo.findOneAndUpsert(
      Mongo.stages(client),
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_STAGE_ID, event.stageId),
        Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, event.stageAttemptId)
      ),
      new Mongo.UpsertBlock<Stage>() {
        @Override
        public Stage update(Stage obj) {
          // we only update information for running stage, since sometimes tasks can finish after
          // stages are complete (see comment above)
          if (obj != null && obj.getStatus() == Stage.Status.ACTIVE) {
            obj.setActiveTasks(activeTasks);
            obj.setCompletedTasks(completedTasks);
            obj.setFailedTasks(failedTasks);
            obj.setMetrics(metrics);
          }
          return obj;
        }
      }
    );
  }

  // == SparkListenerStageSubmitted ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerStageSubmitted event) {
    Mongo.findOneAndUpsert(
      Mongo.stages(client),
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_STAGE_ID, event.stageInfo.stageId),
        Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, event.stageInfo.stageAttemptId)
      ),
      new Mongo.UpsertBlock<Stage>() {
        @Override
        public Stage update(Stage obj) {
          if (obj == null) {
            obj = new Stage();
            obj.setAppId(appId);
          }
          obj.update(event.stageInfo);
          obj.setStatus(Stage.Status.ACTIVE);
          return obj;
        }
      }
    );
  }

  // == SparkListenerStageCompleted ==
  private void processEvent(
      MongoClient client, final String appId, final SparkListenerStageCompleted event) {
    // extract metrics for stage and evict key
    final int activeTasks = stageAgg.getActiveTasks(event.stageInfo.stageId,
      event.stageInfo.stageAttemptId);
    final int completedTasks = stageAgg.getCompletedTasks(event.stageInfo.stageId,
      event.stageInfo.stageAttemptId);
    final int failedTasks = stageAgg.getFailedTasks(event.stageInfo.stageId,
      event.stageInfo.stageAttemptId);
    final Metrics metrics = stageAgg.getMetrics(event.stageInfo.stageId,
      event.stageInfo.stageAttemptId);
    stageAgg.evict(event.stageInfo.stageId, event.stageInfo.stageAttemptId);

    Mongo.findOneAndUpsert(
      Mongo.stages(client),
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_STAGE_ID, event.stageInfo.stageId),
        Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, event.stageInfo.stageAttemptId)
      ),
      new Mongo.UpsertBlock<Stage>() {
        @Override
        public Stage update(Stage obj) {
          if (obj == null) {
            throw new IllegalStateException(
              "Stage is not found for SparkListenerStageCompleted: stageId=" +
              event.stageInfo.stageId + ", stageAttemptId=" + event.stageInfo.stageAttemptId);
          }
          boolean active = obj.getStatus() == Stage.Status.ACTIVE;
          boolean pending = obj.getStatus() == Stage.Status.PENDING;
          obj.setAppId(appId);
          obj.update(event.stageInfo);
          if (active) {
            if (event.stageInfo.failureReason == null) {
              obj.setStatus(Stage.Status.COMPLETED);
            } else {
              obj.setStatus(Stage.Status.FAILED);
            }
            obj.setActiveTasks(activeTasks);
            obj.setCompletedTasks(completedTasks);
            obj.setFailedTasks(failedTasks);
            obj.setMetrics(metrics);
          } else if (pending) {
            // we do not update metrics for skipped stage
            obj.setStatus(Stage.Status.SKIPPED);
          } else {
            LOG.warn("Stage {} ({}) has invalid stage", obj.getStageId(), obj.getStageAttemptId());
            obj.setStatus(Stage.Status.UNKNOWN);
          }
          return obj;
        }
      }
    );
  }
}
