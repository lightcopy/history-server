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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.ApplicationSummary;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
import com.github.lightcopy.history.model.Task;
import com.github.lightcopy.history.process.ExecutorProcess;
import com.github.lightcopy.history.process.InterruptibleThread;
import com.github.lightcopy.history.process.WatchProcess;

/**
 * Process to list applications in event directory and schedule them for processing.
 */
class EventLogManager implements ApiProvider {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogManager.class);
  // shutdown interval in milliseconds, after this, thread will be interrupted
  private static final int INTERRUPT_TIMEOUT_MS = 2000;
  private static final int NUM_EXECUTORS = 2;

  private FileSystem fs;
  private Path root;
  private MongoClient mongo;
  private WatchProcess watchProcess;
  // map of all application logs that have been processed so far (provides guarantee)
  // content of the map should eventually be consistent with database
  private ConcurrentHashMap<String, Application> apps;
  // queue with application logs ready to be processed
  private BlockingQueue<Application> queue;
  // number of executor threads to start
  private ExecutorProcess[] executors;

  public EventLogManager(FileSystem fs, String rootDirectory, MongoClient mongo) {
    this.fs = fs;
    this.root = new Path(rootDirectory);
    this.mongo = mongo;
    this.apps = new ConcurrentHashMap<String, Application>();
    this.queue = new LinkedBlockingQueue<Application>();
    this.executors = new ExecutorProcess[NUM_EXECUTORS];
  }

  public void start() {
    LOG.info("Create index for tables");
    // prepare all tables
    Mongo.buildIndexes(mongo);

    LOG.info("Recover state");
    // clean up state before launching any process:
    // current approach is removing data for all non successfull application logs in all operational
    // tables; we do not allow details for failed or in loading progress application logs in ui.
    final List<String> appsToRemove = new ArrayList<String>();
    Mongo.applications(mongo).find().forEach(new Block<Application>() {
      @Override
      public void apply(Application app) {
        // we put log in map if status is either success or failure. Failed logs will be
        // reconsidered later when actual watch process is triggered, we will compare modification
        // time. Otherwise, we will have to load failed log and reprocess it again to get same
        // failure (can be > 2G file).
        if (app.getLoadStatus() == Application.LoadStatus.LOAD_SUCCESS ||
            app.getLoadStatus() == Application.LoadStatus.LOAD_FAILURE) {
          apps.put(app.getAppId(), app);
        } else {
          appsToRemove.add(app.getAppId());
          LOG.debug("Remove application log {}", app);
        }
      }
    });
    // clean up state for all events based on provided app ids
    Mongo.removeData(mongo, appsToRemove);
    LOG.info("Removed {} applications", appsToRemove.size());

    LOG.info("Discovered {} applications", apps.size());
    LOG.info("Start processes");
    // start processes
    watchProcess = new WatchProcess(fs, root, apps, queue);
    LOG.info("Start watch thread {}", watchProcess);
    watchProcess.start();
    for (int i = 0; i < executors.length; i++) {
      executors[i] = new ExecutorProcess(i, fs, mongo, queue);
      executors[i].start();
      LOG.info("Start executor thread {}", executors[i]);
    }
    LOG.info("Start event log manager");
  }

  public void stop() {
    LOG.info("Stop processes");
    if (watchProcess != null) {
      LOG.info("Stop watch thread {}", watchProcess);
      try {
        EventLogManager.stopThread(watchProcess, INTERRUPT_TIMEOUT_MS);
        // reset properties to null
        watchProcess = null;
      } catch (InterruptedException err) {
        throw new RuntimeException("Intrerrupted thread " + watchProcess, err);
      }
    }
    for (int i = 0; i < executors.length; i++) {
      if (executors[i] != null) {
        LOG.info("Stop executor thread {}", executors[i]);
        try {
          EventLogManager.stopThread(executors[i], INTERRUPT_TIMEOUT_MS);
          executors[i] = null;
        } catch (InterruptedException err) {
          throw new RuntimeException("Intrerrupted thread " + executors[i], err);
        }
      }
    }
    LOG.info("Stop event log manager");
    queue = null;
    apps = null;
  }

  /**
   * Shut down thread gracefully.
   * First ask it to terminate and issue interrupt error after provided timeout, join() is invoked.
   * @param thread thread to stop
   * @param timeout timeout in milliseconds
   */
  private static void stopThread(InterruptibleThread thread, int timeout)
      throws InterruptedException {
    // wait block is 200 milliseconds, we check every block if thread is alive, until full timeout
    // is exhausted - allows to wait just partial timeout
    int waitBlock = Math.min(100, timeout);
    if (thread != null) {
      thread.terminate();
      while (timeout > 0 && thread.isAlive()) {
        timeout -= waitBlock;
        // put current thread to sleep until next check
        Thread.sleep(waitBlock);
      }
      // if thread is still alive, interrupt it with potentially inconsistent state
      if (thread.isAlive()) {
        thread.interrupt();
      }
      thread.join();
    }
  }

  // == API methods ==

  @Override
  public List<Application> applications(int page, int pageSize, String sortBy, boolean asc) {
    final List<Application> list = new ArrayList<Application>();
    // we do not apply any filter when querying applications
    Mongo.page(Mongo.applications(mongo), null, page, pageSize, sortBy, asc).forEach(
      new Block<Application>() {
        @Override
        public void apply(Application app) {
          list.add(app);
        }
      }
    );
    return list;
  }

  @Override
  public Application application(String appId) {
    // this method returns either first application that has appId or null if not found.
    return Mongo.applications(mongo)
      .find(Filters.eq(Application.FIELD_APP_ID, appId))
      .first();
  }

  @Override
  public ApplicationSummary appSummary(String appId) {
    return Mongo.appSummary(mongo)
      .find(Filters.eq(ApplicationSummary.FIELD_APP_ID, appId))
      .first();
  }

  @Override
  public Environment environment(String appId) {
    // this method returns either environment that has appId or null if not found.
    return Mongo.environment(mongo)
      .find(Filters.eq(Environment.FIELD_APP_ID, appId))
      .first();
  }

  @Override
  public List<SQLExecution> sqlExecutions(
      String appId, int page, int pageSize, String sortBy, boolean asc) {
    final List<SQLExecution> list = new ArrayList<SQLExecution>();
    // filter by appId and return sql executions for requested application
    Mongo.page(
      Mongo.sqlExecution(mongo),
      Filters.eq(SQLExecution.FIELD_APP_ID, appId),
      page, pageSize, sortBy, asc)
    .forEach(
      new Block<SQLExecution>() {
        @Override
        public void apply(SQLExecution sql) {
          list.add(sql);
        }
      }
    );
    return list;
  }

  @Override
  public SQLExecution sqlExecution(String appId, int executionId) {
    return Mongo.sqlExecution(mongo)
      .find(
        Filters.and(
          Filters.eq(SQLExecution.FIELD_APP_ID, appId),
          Filters.eq(SQLExecution.FIELD_EXECUTION_ID, executionId)
        )
      )
      .first();
  }

  @Override
  public List<Stage> stages(String appId, int page, int pageSize, String sortBy, boolean asc) {
    final List<Stage> list = new ArrayList<Stage>();
    Mongo.page(
      Mongo.stages(mongo),
      Filters.eq(Stage.FIELD_APP_ID, appId),
      page, pageSize, sortBy, asc)
    .forEach(
      new Block<Stage>() {
        @Override
        public void apply(Stage stage) {
          list.add(stage);
        }
      }
    );
    return list;
  }

  @Override
  public List<Stage> stages(
      String appId, int jobId, int page, int pageSize, String sortBy, boolean asc) {
    final List<Stage> list = new ArrayList<Stage>();
    Mongo.page(
      Mongo.stages(mongo),
      Filters.and(
        Filters.eq(Stage.FIELD_APP_ID, appId),
        Filters.eq(Stage.FIELD_JOB_ID, jobId)
      ),
      page, pageSize, sortBy, asc)
    .forEach(
      new Block<Stage>() {
        @Override
        public void apply(Stage stage) {
          list.add(stage);
        }
      }
    );
    return list;
  }

  @Override
  public Stage stage(String appId, int stageId, int stageAttemptId) {
    return Mongo.stages(mongo)
      .find(
        Filters.and(
          Filters.eq(Stage.FIELD_APP_ID, appId),
          Filters.eq(Stage.FIELD_STAGE_ID, stageId),
          Filters.eq(Stage.FIELD_STAGE_ATTEMPT_ID, stageAttemptId)
        )
      )
      .first();
  }

  @Override
  public List<Task> tasks(String appId, int stageId, int stageAttemptId, int page, int pageSize,
      String sortBy, boolean asc) {
    final List<Task> list = new ArrayList<Task>();
    Mongo.page(
      Mongo.tasks(mongo),
      Filters.and(
        Filters.eq(Task.FIELD_APP_ID, appId),
        Filters.eq(Task.FIELD_STAGE_ID, stageId),
        Filters.eq(Task.FIELD_STAGE_ATTEMPT_ID, stageAttemptId)
      ),
      page, pageSize, sortBy, asc)
    .forEach(
      new Block<Task>() {
        @Override
        public void apply(Task task) {
          list.add(task);
        }
      }
    );
    return list;
  }
}
