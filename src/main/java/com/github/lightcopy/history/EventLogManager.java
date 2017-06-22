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

import com.github.lightcopy.history.process.ExecutorProcess;
import com.github.lightcopy.history.process.WatchProcess;

/**
 * Process to list applications in event directory and schedule them for processing.
 */
class EventLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogManager.class);
  // shutdown interval in milliseconds, after this, thread will be interrupted
  private static final int INTERRUPT_TIMEOUT_MS = 2000;
  private static final int NUM_EXECUTORS = 2;

  private FileSystem fs;
  private Path root;
  private MongoClient mongo;
  private WatchProcess watchProcess;
  // map of all event logs that have been processed so far (provides guarantee)
  // content of the map should eventually be consistent with database
  private ConcurrentHashMap<String, EventLog> eventLogs;
  // queue with event logs ready to be processed
  private BlockingQueue<EventLog> queue;
  // number of executor threads to start
  private ExecutorProcess[] executors;

  public EventLogManager(FileSystem fs, String rootDirectory, MongoClient mongo) {
    this.fs = fs;
    this.root = new Path(rootDirectory);
    this.mongo = mongo;
    this.eventLogs = new ConcurrentHashMap<String, EventLog>();
    this.queue = new LinkedBlockingQueue<EventLog>();
    this.executors = new ExecutorProcess[NUM_EXECUTORS];
  }

  public void start() {
    LOG.info("Create index for tables");
    // prepare all tables
    Mongo.createUniqueIndex(Mongo.eventLogCollection(mongo), EventLog.FIELD_APP_ID);

    LOG.info("Recover state");
    // clean up state before launching any process:
    // current approach is removing data for all non successfull event logs in all operational
    // tables, since we do not show failed or in loading progress event logs in ui.
    final List<EventLog> logsToRemove = new ArrayList<EventLog>();
    Mongo.eventLogCollection(mongo).find().forEach(new Block<EventLog>() {
      @Override
      public void apply(EventLog log) {
        // we put log in map if status is either success or failure. Failed logs will be
        // reconsidered later when actual watch process is triggered, we will compare modification
        // time. Otherwise, we will have to load failed log and reprocess it again to get same
        // failure (can be > 2G file).
        if (log.getStatus() == EventLog.Status.SUCCESS ||
            log.getStatus() == EventLog.Status.FAILURE) {
          eventLogs.put(log.getAppId(), log);
        } else {
          logsToRemove.add(log);
          LOG.debug("Remove log {}", log);
        }
      }
    });
    // clean up state for all events based on provided app ids
    Mongo.removeData(mongo, logsToRemove);
    LOG.info("Removed {} logs", logsToRemove.size());

    LOG.info("Discovered {} logs", eventLogs.size());
    LOG.info("Start processes");
    // start processes
    watchProcess = new WatchProcess(fs, root, eventLogs, queue);
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
        watchProcess.terminate();
        watchProcess.join();
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
          executors[i].terminate();
          Thread.sleep(INTERRUPT_TIMEOUT_MS);
          if (executors[i].isAlive()) {
            executors[i].interrupt();
          }
          executors[i].join();
          executors[i] = null;
        } catch (InterruptedException err) {
          throw new RuntimeException("Intrerrupted thread " + executors[i], err);
        }
      }
    }
    LOG.info("Stop event log manager");
  }
}
