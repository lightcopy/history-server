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

import org.apache.hadoop.fs.FileStatus;
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
import com.github.lightcopy.history.event.SparkListenerExecutorMetricsUpdate;
import com.github.lightcopy.history.event.SparkListenerJobStart;
import com.github.lightcopy.history.event.SparkListenerJobEnd;
import com.github.lightcopy.history.event.SparkListenerSQLExecutionStart;
import com.github.lightcopy.history.event.SparkListenerSQLExecutionEnd;
import com.github.lightcopy.history.event.SparkListenerStageCompleted;
import com.github.lightcopy.history.event.SparkListenerStageSubmitted;
import com.github.lightcopy.history.event.SparkListenerTaskStart;
import com.github.lightcopy.history.event.SparkListenerTaskEnd;
import com.github.lightcopy.history.event.StageInfo;
import com.github.lightcopy.history.event.TaskMetrics;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.ApplicationSummary;
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

  public EventParser(FileSystem fs, MongoClient client, Application app) {
    this.finished = false;
    this.fs = fs;
    this.client = client;
    this.appId = app.getAppId();
    this.appPath = app.getPath();
  }

  /**
   * Parse application logs from file.
   */
  public void parseApplicationLog() throws EventProcessException {
    if (finished) {
      throw new IllegalStateException("Event parser already finished parsing log");
    }
    FSDataInputStream in = null;
    String json = null;
    try {
      FileStatus status = fs.getFileStatus(new Path(appPath));
      in = fs.open(status.getPath());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      while ((json = reader.readLine()) != null) {
        Event event = gson.fromJson(json, Event.class);
        if (event.getEventName() != null) {
          parseJsonEvent(event, json);
          // update progress fraction for application
          updateProgress(in.getPos() * 1.0 / status.getLen());
        } else {
          LOG.warn("Drop event {} for app {}", json, appId);
        }
      }
    } catch (Exception err) {
      throw new EventProcessException(
        "Failed to process events for " + appId + "; err: " + err.getMessage() +
          "; invalid json: " + json, err);
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

  /** Update progress of the application based on new fraction */
  private void updateProgress(double fraction) {
    Application app = Application.getOrCreate(client, appId);
    app.setLoadProgress(fraction);
    app.upsert();
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
      case "SparkListenerExecutorMetricsUpdate":
        processEvent(gson.fromJson(json, SparkListenerExecutorMetricsUpdate.class));
        break;
      default:
        LOG.warn("Unrecognized event {} ", event);
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
    Application app = Application.getOrCreate(client, appId);
    app.setAppName(event.appName);
    app.setStartTime(event.timestamp);
    app.setUser(event.user);
    app.setAppStatus(Application.AppStatus.IN_PROGRESS);
    app.upsert();

    // create entry for application summary
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.upsert();
  }

  // == SparkListenerApplicationEnd ==
  private void processEvent(final SparkListenerApplicationEnd event) {
    Application app = Application.getOrCreate(client, appId);
    app.setEndTime(event.timestamp);
    app.setAppStatus(Application.AppStatus.FINISHED);
    app.upsert();
  }

  // == SparkListenerEnvironmentUpdate ==
  private void processEvent(final SparkListenerEnvironmentUpdate event) {
    Environment env = Environment.getOrCreate(client, appId);
    env.setJvmInformation(event.jvmInformation);
    env.setSparkProperties(event.sparkProperties);
    env.setSystemProperties(event.systemProperties);
    env.setClasspathEntries(event.classpathEntries);
    env.upsert();
  }

  // == SparkListenerSQLExecutionStart ==
  private void processEvent(final SparkListenerSQLExecutionStart event) {
    SQLExecution sql = SQLExecution.getOrCreate(client, appId, event.executionId);
    sql.setDescription(event.description);
    sql.setDetails(event.details);
    sql.setPhysicalPlan(event.physicalPlanDescription);
    sql.setStartTime(event.time);
    sql.updateDuration();
    sql.setStatus(SQLExecution.Status.RUNNING);
    sql.upsert();

    // update summary for active query
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(sql);
    summary.upsert();
  }

  // == SparkListenerSQLExecutionEnd ==
  private void processEvent(final SparkListenerSQLExecutionEnd event) {
    SQLExecution sql = SQLExecution.getOrCreate(client, appId, event.executionId);
    sql.setEndTime(event.time);
    sql.updateDuration();
    sql.setStatus(SQLExecution.Status.COMPLETED);
    sql.upsert();

    // update summary for finished query
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(sql);
    summary.upsert();
  }

  // == SparkListenerJobStart ==
  private void processEvent(final SparkListenerJobStart event) {
    Job job = Job.getOrCreate(client, appId, event.jobId);
    job.setJobName(event.getJobName());
    job.setJobGroup(event.getJobGroup());
    job.setStartTime(event.submissionTime);
    job.setStatus(Job.Status.RUNNING);
    job.setTotalTasks(event.getTotalTasks());

    // Upsert job after all stage updates

    // launch all stages that we used to get total tasks count
    for (final StageInfo info : event.stageInfos) {
      Stage stage = Stage.getOrCreate(client, appId, info.stageId, info.stageAttemptId);
      // update stage only if it is unknown, meaning that we have not submitted it yet
      if (stage.getStatus() == Stage.Status.UNKNOWN) {
        stage.update(info);
        stage.setJobId(event.jobId);
        stage.setStatus(Stage.Status.PENDING);
        stage.upsert();
        job.markStagePending(stage.getStageId(), stage.getStageAttemptId());
      } else {
        // do not upsert here, stage has not been updated
        LOG.warn("Stage {} ({}) was already submitted for application {}, status={}",
          stage.getStageId(), stage.getStageAttemptId(), appId, stage.getStatus());
      }
    }
    job.upsert();

    // update sql execution - add job id to the query
    int queryId = event.getExecutionId();
    if (queryId >= 0) {
      SQLExecution sql = SQLExecution.getOrCreate(client, appId, queryId);
      sql.addJobId(event.jobId);
      sql.upsert();
    }

    // update application summary
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(job);
    summary.upsert();
  }

  // == SparkListenerJobEnd ==
  private void processEvent(final SparkListenerJobEnd event) {
    Job job = Job.getOrCreate(client, appId, event.jobId);
    job.setEndTime(event.completionTime);
    job.updateDuration();
    if (event.jobResult.isSuccess()) {
      job.setStatus(Job.Status.SUCCEEDED);
      job.setErrorDescription("");
      job.setErrorDetails(null);
    } else {
      job.setStatus(Job.Status.FAILED);
      job.setErrorDescription(event.jobResult.getErrorDescription());
      job.setErrorDetails(event.jobResult.getErrorDetails());
    }
    // upsert job at the end of the method, since we need to update stages as well

    // mark all pending stages for the job as skipped
    // first, we find all stages that belong to the job
    // TODO: Update this block, so it is part of Mongo or Job, or Stage class.
    final ArrayList<Stage> stagesToUpdate = new ArrayList<Stage>();
    Mongo.stages(client).find(
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_JOB_ID, job.getJobId()),
        Filters.eq(Stage.FIELD_STATUS, Stage.Status.PENDING.name())
      )
    ).forEach(new Block<Stage>() {
      @Override
      public void apply(Stage stage) {
        // TODO: remove this workaround, should be already set when loaded
        stage.setMongoClient(client);
        stagesToUpdate.add(stage);
      }
    });

    // mark all pending stages as skipped
    for (Stage stage : stagesToUpdate) {
      stage.setStatus(Stage.Status.SKIPPED);
      stage.upsert();

      // do not update metrics for skipped stages
      job.markStageSkipped(stage.getStageId(), stage.getStageAttemptId());
      job.incSkippedTasks(stage.getTotalTasks());
    }
    LOG.debug("Updated {} stages as SKIPPED for job {} in application {}",
      stagesToUpdate.size(), job.getJobId(), appId);

    job.upsert();

    // update application summary
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(job);
    summary.upsert();
  }

  // == SparkListenerStageSubmitted ==
  private void processEvent(final SparkListenerStageSubmitted event) {
    // if we encounter attempt > 0, this might not be launched by job start event, therefore we
    // reconstruct job link manually when getting stage (see Stage.getOrCreate for more info)
    Stage stage = Stage.getOrCreate(client, appId,
      event.stageInfo.stageId, event.stageInfo.stageAttemptId);
    stage.update(event.stageInfo);
    stage.setStatus(Stage.Status.ACTIVE);
    stage.setJobGroup(event.getJobGroup());
    stage.upsert();

    // Update job
    Job job = Job.getOrCreate(client, appId, stage.getJobId());
    job.markStageActive(stage.getStageId(), stage.getStageAttemptId());
    job.upsert();

    // update application summary
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(job);
    summary.upsert();
  }

  // == SparkListenerStageCompleted ==
  private void processEvent(final SparkListenerStageCompleted event) {
    Stage stage = Stage.getOrCreate(client, appId,
      event.stageInfo.stageId, event.stageInfo.stageAttemptId);
    // fetch status before we update main info
    boolean active = stage.getStatus() == Stage.Status.ACTIVE;
    boolean pending = stage.getStatus() == Stage.Status.PENDING;
    stage.update(event.stageInfo);
    if (active) {
      if (event.stageInfo.isSuccess()) {
        stage.setStatus(Stage.Status.COMPLETED);
      } else {
        stage.setStatus(Stage.Status.FAILED);
      }
    } else if (pending) {
      stage.setStatus(Stage.Status.SKIPPED);
    } else {
      LOG.warn("Stage {} ({}) has invalid stage", stage.getStageId(), stage.getStageAttemptId());
      stage.setStatus(Stage.Status.UNKNOWN);
    }
    stage.upsert();

    Job job = Job.getOrCreate(client, appId, stage.getJobId());
    switch (stage.getStatus()) {
      case COMPLETED:
        job.markStageCompleted(stage.getStageId(), stage.getStageAttemptId());
        break;
      case FAILED:
        job.markStageFailed(stage.getStageId(), stage.getStageAttemptId());
        break;
      case SKIPPED:
        job.markStageSkipped(stage.getStageId(), stage.getStageAttemptId());
        break;
      default:
        // ignore update completely, remove stage from job
        job.unlinkStage(stage.getStageId(), stage.getStageAttemptId());
        break;
    }
    job.upsert();

    // update application summary
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(job);
    summary.upsert();
  }

  // == SparkListenerTaskStart ==
  private void processEvent(final SparkListenerTaskStart event) {
    Task task = Task.getOrCreate(client, appId, event.taskInfo.taskId);
    task.setStageId(event.stageId);
    task.setStageAttemptId(event.stageAttemptId);
    task.update(event.taskInfo);
    task.upsert();
    // Update stage
    Stage stage = Stage.getOrCreate(client, appId, event.stageId, event.stageAttemptId);
    stage.incActiveTasks();
    stage.upsert();
    // Update job
    Job job = Job.getOrCreate(client, appId, stage.getJobId());
    job.incActiveTasks();
    job.upsert();
    // Update executor
    Executor exc = Executor.getOrCreate(client, appId, event.taskInfo.executorId);
    exc.incActiveTasks();
    exc.upsert();
  }

  // == SparkListenerTaskEnd ==
  private void processEvent(final SparkListenerTaskEnd event) {
    if (event.taskInfo != null && event.stageAttemptId != -1) {
      Task task = Task.getOrCreate(client, appId, event.taskInfo.taskId);
      // Event can contain null task metrics; in this case we just update task information, also
      // skip any metrics updates for stage and job.
      Metrics delta = null;
      if (event.taskMetrics != null) {
        delta = Metrics.fromTaskMetrics(event.taskMetrics).delta(task.getMetrics());
      }
      // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
      // completion event is for. For now we allow processing of task, it will be assigned to
      // stage -1 which does not exist and we never query by negative attempt
      task.setStageId(event.stageId);
      task.setStageAttemptId(event.stageAttemptId);
      task.update(event.taskInfo);
      task.update(event.taskEndReason);
      // overwrite task metrics
      if (delta != null) {
        task.update(event.taskMetrics);
      }
      boolean taskSucceeded = task.getStatus() == Task.Status.SUCCESS;
      task.upsert();

      // Update stage
      Stage stage = Stage.getOrCreate(client, appId, event.stageId, event.stageAttemptId);
      stage.decActiveTasks();
      if (taskSucceeded) {
        stage.incCompletedTasks();
      } else {
        stage.incFailedTasks();
      }
      if (delta != null) {
        stage.updateMetrics(delta);
      }
      stage.upsert();

      // Update job
      Job job = Job.getOrCreate(client, appId, stage.getJobId());
      job.decActiveTasks();
      if (taskSucceeded) {
        job.incCompletedTasks();
      } else {
        job.incFailedTasks();
      }
      if (delta != null) {
        job.updateMetrics(delta);
      }
      job.upsert();

      // Update executor
      Executor exc = Executor.getOrCreate(client, appId, event.taskInfo.executorId);
      exc.decActiveTasks();
      if (taskSucceeded) {
        exc.incCompletedTasks();
      } else {
        exc.incFailedTasks();
      }
      if (delta != null) {
        exc.updateMetrics(delta);
      }
      exc.incTaskTime(task.getDuration());
      exc.upsert();
    }
  }

  // == SparkListenerExecutorAdded ==
  private void processEvent(final SparkListenerExecutorAdded event) {
    Executor exc = Executor.getOrCreate(client, appId, event.executorId);
    exc.setHost(event.info.host);
    exc.setCores(event.info.totalCores);
    exc.setStartTime(event.timestamp);
    exc.setStatus(Executor.Status.ACTIVE);
    exc.setLogs(event.info.logUrls);
    exc.upsert();

    // update summary for active executor
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(exc);
    summary.upsert();
  }

  // == SparkListenerExecutorRemoved ==
  private void processEvent(final SparkListenerExecutorRemoved event) {
    Executor exc = Executor.getOrCreate(client, appId, event.executorId);
    exc.setStatus(Executor.Status.REMOVED);
    exc.setFailureReason(event.reason);
    exc.setEndTime(event.timestamp);
    exc.updateDuration();
    exc.upsert();

    // update summary for removed executor
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(exc);
    summary.upsert();
  }

  // == SparkListenerBlockManagerAdded ==
  private void processEvent(final SparkListenerBlockManagerAdded event) {
    Executor exc = Executor.getOrCreate(client, appId, event.blockManagerId.executorId);
    exc.setHost(event.blockManagerId.host);
    exc.setPort(event.blockManagerId.port);
    exc.setMaxMemory(event.maximumMemory);
    exc.setStartTime(event.timestamp);
    exc.setStatus(Executor.Status.ACTIVE);
    exc.upsert();

    // update summary for active executor
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(exc);
    summary.upsert();
  }

  // == SparkListenerBlockManagerRemoved ==
  private void processEvent(final SparkListenerBlockManagerRemoved event) {
    Executor exc = Executor.getOrCreate(client, appId, event.blockManagerId.executorId);
    exc.setStatus(Executor.Status.REMOVED);
    exc.setEndTime(event.timestamp);
    exc.updateDuration();
    exc.upsert();

    // update summary for removed executor
    ApplicationSummary summary = ApplicationSummary.getOrCreate(client, appId);
    summary.update(exc);
    summary.upsert();
  }

  // == SparkListenerExecutorMetricsUpdate ==
  private void processEvent(final SparkListenerExecutorMetricsUpdate event) {
    // TODO: update executor-per-stage metrics
    Task task = Task.getOrCreate(client, appId, event.metrics.taskId);
    Metrics oldMetrics = task.getMetrics().copy();
    Metrics newMetrics =
      Metrics.fromTaskMetrics(TaskMetrics.fromAccumulableInfo(event.metrics.updates));
    Metrics delta = newMetrics.delta(oldMetrics);

    // overwrite task metrics
    task.setMetrics(newMetrics);
    task.upsert();

    // update metrics for stage
    Stage stage = Stage.getOrCreate(client, appId,
      event.metrics.stageId, event.metrics.stageAttemptId);
    // update stage metrics as current metrics - old task metrics
    stage.updateMetrics(delta);
    stage.upsert();

    // update metrics for job
    Job job = Job.getOrCreate(client, appId, stage.getJobId());
    job.updateMetrics(delta);
    job.upsert();

    // Update metrics for executor
    Executor exc = Executor.getOrCreate(client, appId, event.executorId);
    exc.updateMetrics(delta);
    exc.upsert();
  }
}
