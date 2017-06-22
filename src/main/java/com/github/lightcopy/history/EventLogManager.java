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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

/**
 * Process to list applications in event directory and schedule them for processing.
 */
class EventLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogManager.class);

  private FileSystem fs;
  private Path root;
  private MongoClient mongo;
  private WatchProcess watchProcess;
  private Thread watchProcessThread;
  // map of all event logs that have been processed so far (provides guarantee)
  // content of the map should eventually be consistent with database
  private ConcurrentHashMap<String, EventLog> eventLogs;
  // queue with event logs ready to be processed
  private BlockingQueue<EventLog> queue;

  public EventLogManager(FileSystem fs, String rootDirectory, MongoClient mongo) {
    this.fs = fs;
    this.root = new Path(rootDirectory);
    this.mongo = mongo;
    this.eventLogs = new ConcurrentHashMap<String, EventLog>();
    this.queue = new LinkedBlockingQueue<EventLog>();
  }

  public void start() {
    LOG.info("Create index for tables");
    // prepare all tables
    Mongo.createUniqueIndex(Mongo.eventLogCollection(mongo), EventLog.FIELD_APP_ID);

    LOG.info("Recover state");
    // clean up state before launching any process:
    // current approach is removing data for all non successfull event logs in all operational
    // tables, since we do not show failed or in loading progress event logs in ui.
    List<String> logsToRemove = new ArrayList<String>();
    Mongo.eventLogCollection(mongo).find().forEach(new Block<EventLog>() {
      @Override
      public void apply(EventLog log) {
        if (log.getStatus() == EventLog.Status.SUCCESS) {
          eventLogs.put(log.getAppId(), log);
        } else {
          logsToRemove.add(log.getAppId());
          LOG.info("Remove log {}", log);
        }
      }
    });
    Mongo.eventLogCollection(mongo).deleteMany(Filters.all(EventLog.FIELD_APP_ID, logsToRemove));

    LOG.info("Start processes");
    // start processes
    watchProcess = new WatchProcess(fs, root, eventLogs, queue);
    watchProcessThread = new Thread(watchProcess);
    LOG.info("Start watch thread {}", watchProcessThread);
    watchProcessThread.start();
    LOG.info("Start event log manager");
  }

  public void stop() {
    LOG.info("Stop processes");
    if (watchProcessThread != null) {
      LOG.info("Stop watch thread {}", watchProcessThread);
      try {
        watchProcess.terminate();
        watchProcessThread.join();
        // reset properties to null
        watchProcessThread = null;
        watchProcess = null;
      } catch (InterruptedException err) {
        throw new RuntimeException("Intrerrupted thread " + watchProcessThread, err);
      }
    }
    LOG.info("Stop event log manager");
  }

  /**
   * Watch process to list application files in root directory.
   *
   */
  static class WatchProcess implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WatchProcess.class);
    // polling interval in milliseconds = 1.25 sec + random interval
    public static final int POLLING_INTERVAL_MS = 1250;

    private final FileSystem fs;
    private final Path root;
    private final ConcurrentHashMap<String, EventLog> eventLogs;
    private final BlockingQueue<EventLog> queue;
    private final Random rand;
    private volatile boolean stopped;

    public WatchProcess(FileSystem fs, Path root, ConcurrentHashMap<String, EventLog> eventLogs,
        BlockingQueue<EventLog> queue) {
      this.fs = fs;
      this.root = root;
      this.eventLogs = eventLogs;
      this.queue = queue;
      this.rand = new Random();
      this.stopped = false;
    }

    @Override
    public void run() {
      while (!this.stopped) {
        try {
          FileStatus[] statuses = fs.listStatus(this.root);
          if (statuses != null && statuses.length > 0) {
            for (FileStatus status : statuses) {
              if (status.isFile()) {
                EventLog log = EventLog.fromStatus(status);
                LOG.debug("Found event log {}", log);
                // we only schedule applications that are newly added or existing with failure
                // status and have been updated since.
                if (log.inProgress()) {
                  LOG.debug("Discard in-progress log {}", log);
                } else {
                  EventLog existingLog = eventLogs.get(log.getAppId());
                  if (existingLog == null) {
                    // new application log - add to the map and queue
                    LOG.info("Add log {} for processing", log);
                    eventLogs.put(log.getAppId(), log);
                    queue.put(log);
                  } else if (existingLog.getStatus() == EventLog.Status.FAILURE &&
                      existingLog.getModificationTime() < log.getModificationTime()) {
                    // check status of the log, if status if failure, but modification time is
                    // greater than the one existin log has, update status and add to processing
                    eventLogs.replace(log.getAppId(), log);
                    queue.put(log);
                  } else {
                    LOG.debug("Discard existing log {}", existingLog);
                  }
                }
              }
            }
          }
          long interval = POLLING_INTERVAL_MS + rand.nextInt(POLLING_INTERVAL_MS);
          LOG.debug("Waiting to poll, interval={}", interval);
          Thread.sleep(interval);
        } catch (Exception err) {
          LOG.error("Thread interrupted", err);
          this.stopped = true;
        }
      }
    }

    /** Whether or not watch thread is stopped */
    public boolean isStopped() {
      return this.stopped;
    }

    /** Mark watch thread as terminated */
    public void terminate() {
      this.stopped = true;
    }
  }
}
