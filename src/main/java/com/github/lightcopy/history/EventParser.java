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
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

import com.github.lightcopy.history.event.Event;
import com.github.lightcopy.history.event.SparkListenerApplicationStart;
import com.github.lightcopy.history.event.SparkListenerApplicationEnd;
import com.github.lightcopy.history.event.SparkListenerBlockManagerAdded;
import com.github.lightcopy.history.event.SparkListenerBlockManagerRemoved;
import com.github.lightcopy.history.event.SparkListenerEnvironmentUpdate;
import com.github.lightcopy.history.event.SparkListenerExecutorAdded;
import com.github.lightcopy.history.event.SparkListenerExecutorRemoved;
import com.github.lightcopy.history.event.SparkListenerJobStart;
import com.github.lightcopy.history.event.SparkListenerJobEnd;
import com.github.lightcopy.history.event.SparkListenerSQLExecutionStart;
import com.github.lightcopy.history.event.SparkListenerSQLExecutionEnd;
import com.github.lightcopy.history.event.SparkListenerStageCompleted;
import com.github.lightcopy.history.event.SparkListenerStageSubmitted;
import com.github.lightcopy.history.event.SparkListenerTaskStart;
import com.github.lightcopy.history.event.SparkListenerTaskEnd;
import com.github.lightcopy.history.event.StageInfo;

import com.github.lightcopy.history.model.agg.ApplicationSummary;
import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Executor;
import com.github.lightcopy.history.model.Job;
import com.github.lightcopy.history.model.Metrics;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
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

  // whether or not current event parser finished parsing log
  private volatile boolean finished;
  private final FileSystem fs;
  private final MongoClient client;
  // separate application
  private final String appId;
  private final String appPath;
  private final ApplicationSummary summary;

  public EventParser(FileSystem fs, MongoClient client, Application app) {
    this.finished = false;
    this.fs = fs;
    this.client = client;
    this.appId = app.getAppId();
    this.appPath = app.getPath();
    // aggregated metrics for application/stages/jobs/executors
    this.summary = new ApplicationSummary();
  }

  /**
   * Parse application logs from file.
   */
  public void parseApplicationLog() throws EventProcessException {
    if (finished) {
      throw new IllegalStateException("Event parser already finished parsing log");
    }
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(appPath));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String json;
      while ((json = reader.readLine()) != null) {
        Event event = gson.fromJson(json, Event.class);
        if (event.getEventName() != null) {
          parseJsonEvent(event, json);
        } else {
          LOG.warn("Drop event {} for app {}", json, appId);
        }
      }
    } catch (Exception err) {
      throw new EventProcessException(
        "Failed to process events for " + appId + "; err: " + err.getMessage(), err);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ioe) {
          // no-op
        }
      }
      // always mark event parser as finished
      finished = true;
    }
  }

  /** Parse individual event from json string */
  private void parseJsonEvent(Event event, String json) {
    switch (event.getEventName()) {
      case "SparkListenerApplicationStart":
        processEvent(gson.fromJson(json, SparkListenerApplicationStart.class));
        break;
      case "SparkListenerApplicationEnd":
        processEvent(gson.fromJson(json, SparkListenerApplicationEnd.class));
        break;
      case "SparkListenerEnvironmentUpdate":
        processEvent(gson.fromJson(json, SparkListenerEnvironmentUpdate.class));
        break;
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
        processEvent(gson.fromJson(json, SparkListenerSQLExecutionStart.class));
        break;
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
        processEvent(gson.fromJson(json, SparkListenerSQLExecutionEnd.class));
        break;
      case "SparkListenerTaskStart":
        processEvent(gson.fromJson(json, SparkListenerTaskStart.class));
        break;
      case "SparkListenerTaskEnd":
        processEvent(gson.fromJson(json, SparkListenerTaskEnd.class));
        break;
      case "SparkListenerStageSubmitted":
        processEvent(gson.fromJson(json, SparkListenerStageSubmitted.class));
        break;
      case "SparkListenerStageCompleted":
        processEvent(gson.fromJson(json, SparkListenerStageCompleted.class));
        break;
      case "SparkListenerJobStart":
        processEvent(gson.fromJson(json, SparkListenerJobStart.class));
        break;
      case "SparkListenerJobEnd":
        processEvent(gson.fromJson(json, SparkListenerJobEnd.class));
        break;
      case "SparkListenerExecutorAdded":
        processEvent(gson.fromJson(json, SparkListenerExecutorAdded.class));
        break;
      case "SparkListenerExecutorRemoved":
        processEvent(gson.fromJson(json, SparkListenerExecutorRemoved.class));
        break;
      case "SparkListenerBlockManagerAdded":
        processEvent(gson.fromJson(json, SparkListenerBlockManagerAdded.class));
        break;
      case "SparkListenerBlockManagerRemoved":
        processEvent(gson.fromJson(json, SparkListenerBlockManagerRemoved.class));
        break;
      default:
        LOG.warn("Unrecongnized event {} ", event);
        break;
    }
  }

  // == Processing methods for listener events ==

  // == SparkListenerApplicationStart ==
  private void processEvent(final SparkListenerApplicationStart event) {
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
  private void processEvent(final SparkListenerApplicationEnd event) {
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
  private void processEvent(final SparkListenerEnvironmentUpdate event) {
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
  private void processEvent(final SparkListenerSQLExecutionStart event) {
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
  private void processEvent(final SparkListenerSQLExecutionEnd event) {
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
  private void processEvent(final SparkListenerTaskStart event) {
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
    summary.incActiveTasks(event.stageId, event.stageAttemptId);
  }

  // == SparkListenerTaskEnd ==
  private void processEvent(final SparkListenerTaskEnd event) {
    final int stageId = event.stageId;
    final int stageAttemptId = event.stageAttemptId;
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
          obj.setStageId(stageId);
          obj.setStageAttemptId(stageAttemptId);
          obj.update(event.taskInfo);
          obj.update(event.taskEndReason);
          obj.update(event.taskMetrics);
          return obj;
        }
      }
    );

    // update stage info with current metrics and stats snapshot
    summary.decActiveTasks(stageId, stageAttemptId);
    summary.incMetrics(stageId, stageAttemptId, Metrics.fromTaskMetrics(event.taskMetrics));
    if (event.taskEndReason.isSuccess()) {
      summary.incCompletedTasks(stageId, stageAttemptId);
    } else {
      summary.incFailedTasks(stageId, stageAttemptId);
    }
  }

  // == SparkListenerStageSubmitted ==
  private void processEvent(final SparkListenerStageSubmitted event) {
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
          // update summary for stage
          summary.markActive(event.stageInfo.stageId, event.stageInfo.stageAttemptId);
          return obj;
        }
      }
    );
  }

  // == SparkListenerStageCompleted ==
  private void processEvent(final SparkListenerStageCompleted event) {
    final int stageId = event.stageInfo.stageId;
    final int stageAttemptId = event.stageInfo.stageAttemptId;
    Mongo.findOneAndUpsert(
      Mongo.stages(client),
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_STAGE_ID, stageId),
        Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, stageAttemptId)
      ),
      new Mongo.UpsertBlock<Stage>() {
        @Override
        public Stage update(Stage obj) {
          if (obj == null) {
            throw new IllegalStateException(
              "Stage is not found for SparkListenerStageCompleted: stageId=" +
              stageId + ", stageAttemptId=" + stageAttemptId);
          }
          boolean active = obj.getStatus() == Stage.Status.ACTIVE;
          boolean pending = obj.getStatus() == Stage.Status.PENDING;
          obj.setAppId(appId);
          obj.update(event.stageInfo);
          if (active) {
            if (event.stageInfo.isSuccess()) {
              obj.setStatus(Stage.Status.COMPLETED);
              summary.markCompleted(stageId, stageAttemptId);
            } else {
              obj.setStatus(Stage.Status.FAILED);
              summary.markFailed(stageId, stageAttemptId);
            }
          } else if (pending) {
            // we do not update metrics for skipped stage
            obj.setStatus(Stage.Status.SKIPPED);
            summary.markSkipped(stageId, stageAttemptId);
          } else {
            LOG.warn("Stage {} ({}) has invalid stage", stageId, stageAttemptId);
            obj.setStatus(Stage.Status.UNKNOWN);
          }
          return obj;
        }
      }
    );
  }

  // == SparkListenerJobStart ==
  private void processEvent(final SparkListenerJobStart event) {
    Mongo.findOneAndUpsert(
      Mongo.jobs(client),
      Filters.and(
        Filters.eq(Job.FIELD_APP_ID, appId),
        Filters.eq(Job.FIELD_JOB_ID, event.jobId)
      ),
      new Mongo.UpsertBlock<Job>() {
        @Override
        public Job update(Job obj) {
          if (obj == null) {
            obj = new Job();
          }
          obj.setAppId(appId);
          obj.setJobId(event.jobId);
          obj.setJobName(event.getJobName());
          obj.setStartTime(event.submissionTime);
          obj.setStatus(Job.Status.RUNNING);
          // update stage information
          obj.setTotalTasks(event.getTotalTasks());
          summary.markJobRunning(event.jobId);
          return obj;
        }
      }
    );

    // launch all stages that we used to get total tasks count
    for (final StageInfo info : event.stageInfos) {
      Mongo.findOneAndUpsert(
        Mongo.stages(client),
        Filters.and(
          Filters.eq(Stage.FIELD_APP_ID, appId),
          Filters.eq(Stage.FIELD_STAGE_ID, info.stageId),
          Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, info.stageAttemptId)
        ),
        new Mongo.UpsertBlock<Stage>() {
          @Override
          public Stage update(Stage obj) {
            if (obj == null) {
              obj = new Stage();
              obj.setAppId(appId);
            }
            // update stage only if it is unknown, meaning that we have not submitted it yet
            if (obj.getStatus() == Stage.Status.UNKNOWN) {
              obj.update(info);
              obj.setStatus(Stage.Status.PENDING);
              summary.markPending(info.stageId, info.stageAttemptId);
              summary.addStageToJob(event.jobId, info.stageId);
            } else {
              LOG.warn("Stage {} ({}) was already submitted for application {}, status={}",
                obj.getStageId(), obj.getStageAttemptId(), appId, obj.getStatus());
            }
            return obj;
          }
        }
      );
    }

    // update sql execution - add job id to the query
    int queryId = event.getExecutionId();
    if (queryId >= 0) {
      Mongo.findOneAndUpsert(
        Mongo.sqlExecution(client),
        Filters.and(
          Filters.eq(SQLExecution.FIELD_APP_ID, appId),
          Filters.eq(SQLExecution.FIELD_EXECUTION_ID, queryId)
        ),
        new Mongo.UpsertBlock<SQLExecution>() {
          @Override
          public SQLExecution update(SQLExecution query) {
            if (query != null) {
              query.addJobId(event.jobId);
            }
            return query;
          }
        }
      );
    }
  }

  // == SparkListenerJobEnd ==
  private void processEvent(final SparkListenerJobEnd event) {
    Mongo.findOneAndUpsert(
      Mongo.jobs(client),
      Filters.and(
        Filters.eq(Job.FIELD_APP_ID, appId),
        Filters.eq(Job.FIELD_JOB_ID, event.jobId)
      ),
      new Mongo.UpsertBlock<Job>() {
        @Override
        public Job update(Job obj) {
          if (obj == null) {
            LOG.warn("Job is null for application {} and jobId {}", appId, event.jobId);
            obj = new Job();
          }
          obj.setAppId(appId);
          obj.setJobId(event.jobId);
          obj.setEndTime(event.completionTime);
          obj.updateDuration();
          if (event.jobResult.isSuccess()) {
            obj.setStatus(Job.Status.SUCCEEDED);
            obj.setErrorDescription("");
            obj.setErrorDetails(null);
            summary.markJobSucceeded(event.jobId);
          } else {
            obj.setStatus(Job.Status.FAILED);
            obj.setErrorDescription(event.jobResult.getDescription());
            obj.setErrorDetails(event.jobResult.getDetails());
            summary.markJobFailed(event.jobId);
          }
          return obj;
        }
      }
    );

    // mark all pending stages for the job as skipped
    // first, we find all stages that belong to the job
    // TODO: optimize this block of code to do atomic update for stage
    final ArrayList<Stage> stagesToUpdate = new ArrayList<Stage>();
    Mongo.stages(client).find(
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_JOB_ID, event.jobId)
      )
    ).forEach(new Block<Stage>() {
      @Override
      public void apply(Stage stage) {
        stagesToUpdate.add(stage);
      }
    });

    // mark all pending stages as skipped
    for (final Stage stage : stagesToUpdate) {
      if (stage.getStatus() == Stage.Status.PENDING) {
        Mongo.findOneAndUpsert(
          Mongo.stages(client),
          Filters.and(
            Filters.eq(Stage.FIELD_APP_ID, appId),
            Filters.eq(Stage.FIELD_STAGE_ID, stage.getStageId()),
            Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, stage.getStageAttemptId())
          ),
          new Mongo.UpsertBlock<Stage>() {
            @Override
            public Stage update(Stage obj) {
              if (obj != null && obj.getStatus() == Stage.Status.PENDING) {
                // do not update metrics for skipped stage
                obj.setStatus(Stage.Status.SKIPPED);
                summary.markSkipped(stage.getStageId(), stage.getStageAttemptId());
              }
              return obj;
            }
          }
        );
      }
    }
  }

  // == SparkListenerExecutorAdded ==
  private void processEvent(final SparkListenerExecutorAdded event) {
    Mongo.findOneAndUpsert(
      Mongo.executors(client),
      Filters.and(
        Filters.eq(Executor.FIELD_APP_ID, appId),
        Filters.eq(Executor.FIELD_EXECUTOR_ID, event.executorId)
      ),
      new Mongo.UpsertBlock<Executor>() {
        @Override
        public Executor update(Executor obj) {
          if (obj == null) {
            obj = new Executor();
          }
          obj.setAppId(appId);
          obj.setExecutorId(event.executorId);
          obj.setHost(event.info.host);
          obj.setCores(event.info.totalCores);
          obj.setStartTime(event.timestamp);
          obj.setStatus(Executor.Status.ACTIVE);
          obj.setLogs(event.info.logUrls);
          return obj;
        }
      }
    );
  }

  // == SparkListenerExecutorRemoved ==
  private void processEvent(final SparkListenerExecutorRemoved event) {
    Mongo.findOneAndUpsert(
      Mongo.executors(client),
      Filters.and(
        Filters.eq(Executor.FIELD_APP_ID, appId),
        Filters.eq(Executor.FIELD_EXECUTOR_ID, event.executorId)
      ),
      new Mongo.UpsertBlock<Executor>() {
        @Override
        public Executor update(Executor obj) {
          // ignore null updates, if any
          if (obj != null) {
            obj.setStatus(Executor.Status.REMOVED);
            obj.setFailureReason(event.reason);
            obj.setEndTime(event.timestamp);
            obj.updateDuration();
          }
          return obj;
        }
      }
    );
  }

  // == SparkListenerBlockManagerAdded ==
  private void processEvent(final SparkListenerBlockManagerAdded event) {
    Mongo.findOneAndUpsert(
      Mongo.executors(client),
      Filters.and(
        Filters.eq(Executor.FIELD_APP_ID, appId),
        Filters.eq(Executor.FIELD_EXECUTOR_ID, event.blockManagerId.executorId)
      ),
      new Mongo.UpsertBlock<Executor>() {
        @Override
        public Executor update(Executor obj) {
          if (obj == null) {
            obj = new Executor();
          }
          obj.setAppId(appId);
          obj.setExecutorId(event.blockManagerId.executorId);
          obj.setHost(event.blockManagerId.host);
          obj.setPort(event.blockManagerId.port);
          obj.setMaxMemory(event.maximumMemory);
          obj.setStartTime(event.timestamp);
          obj.setStatus(Executor.Status.ACTIVE);
          return obj;
        }
      }
    );
  }

  // == SparkListenerBlockManagerRemoved ==
  private void processEvent(final SparkListenerBlockManagerRemoved event) {
    Mongo.findOneAndUpsert(
      Mongo.executors(client),
      Filters.and(
        Filters.eq(Executor.FIELD_APP_ID, appId),
        Filters.eq(Executor.FIELD_EXECUTOR_ID, event.blockManagerId.executorId)
      ),
      new Mongo.UpsertBlock<Executor>() {
        @Override
        public Executor update(Executor obj) {
          if (obj == null) {
            // having this is strange, since we would receive blockAdded event first
            obj = new Executor();
            obj.setAppId(appId);
            obj.setExecutorId(event.blockManagerId.executorId);
            obj.setHost(event.blockManagerId.host);
            obj.setPort(event.blockManagerId.port);
          }
          obj.setStatus(Executor.Status.REMOVED);
          obj.setEndTime(event.timestamp);
          obj.updateDuration();
          return obj;
        }
      }
    );
  }
}
