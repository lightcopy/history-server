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

package com.github.lightcopy.history.process;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lightcopy.history.EventProcessException;
import com.github.lightcopy.history.model.EventLog;

/**
 * Watch process to list application files in root directory.
 *
 */
public class WatchProcess extends InterruptibleThread {
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
          LOG.debug("Found {} statuses by listing directory", statuses.length);
          for (FileStatus status : statuses) {
            if (status.isFile()) {
              EventLog log = EventLog.fromStatus(status);
              LOG.debug("Found event log {}", log.getAppId());
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
                  // greater than the one existing log has, update status and add to processing
                  LOG.info("Reprocess log {}", log);
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
      } catch (InterruptedException err) {
        LOG.info("{} - thread stopped with inconsistent state", this);
        this.stopped = true;
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

  @Override
  public void terminate() {
    this.stopped = true;
  }

  @Override
  public String toString() {
    return "WatchProcess";
  }
}
