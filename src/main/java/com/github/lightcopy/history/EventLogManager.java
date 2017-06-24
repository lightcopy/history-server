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

import com.github.lightcopy.history.model.EventLog;
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
    Mongo.buildIndexes(mongo);

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
  public List<EventLog> eventLogs() {
    return new ArrayList<EventLog>();
  }
}
