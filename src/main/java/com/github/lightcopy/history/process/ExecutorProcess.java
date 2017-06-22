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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import com.github.lightcopy.history.EventLog;
import com.github.lightcopy.history.Mongo;

/**
 * Executor process to parse event logs.
 * Executor thread is allowed and can be interrupted, this should be treated as normal shutdown
 * without state cleanup - this will be done on next start.
 */
public class ExecutorProcess extends InterruptibleThread {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorProcess.class);
  // polling interval in milliseconds = 0.5 sec + random interval
  public static final int POLLING_INTERVAL_MS = 500;

  // unique executor id
  private int id;
  private FileSystem fs;
  private MongoClient mongo;
  private BlockingQueue<EventLog> queue;
  private final Random rand;
  private volatile boolean stopped;

  public ExecutorProcess(int id, FileSystem fs, MongoClient mongo, BlockingQueue<EventLog> queue) {
    this.id = id;
    this.fs = fs;
    this.mongo = mongo;
    this.queue = queue;
    this.rand = new Random();
    this.stopped = false;
  }

  @Override
  public void run() {
    EventLog log;
    while (!this.stopped) {
      try {
        while ((log = queue.poll(POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS)) != null) {
          // executor only runs event logs that are marked as PROGRESS
          if (log.getStatus() != EventLog.Status.IN_PROGRESS) {
            throw new RuntimeException(
              "Scheduled event log " + log + " is not marked for progress");
          }
          LOG.debug("{} - prepare state for {}", id, log);
          // always clean up data before processing new app id
          // since we can end up with partial if log was in progress before shutdown or failed to
          // process
          Mongo.removeData(mongo, log);
          // initial insert of the processing event log
          Mongo.eventLogCollection(mongo).insertOne(log);

          LOG.info("{} - processing {}", id, log);
          // instead of processing just invoke sleep
          try {
            Thread.sleep(10000);
            if (rand.nextDouble() > 0.3) {
              throw new EventProcessException("Test");
            }
            log.updateStatus(EventLog.Status.SUCCESS);
          } catch (EventProcessException err) {
            LOG.error("Failed to process log {}", log);
            log.updateStatus(EventLog.Status.FAILURE);
          }

          UpdateOptions opts = new UpdateOptions().upsert(false);
          UpdateResult res = Mongo.eventLogCollection(mongo)
            .replaceOne(Filters.eq(EventLog.FIELD_APP_ID, log.getAppId()), log, opts);
          // roll back update if write failed
          if (!res.wasAcknowledged()) {
            LOG.error("Failed to update log {}, res={}", log, res);
            log.updateStatus(EventLog.Status.FAILURE);
          } else {
            LOG.info("Updated log {}", log);
          }
        }
        long interval = POLLING_INTERVAL_MS + rand.nextInt(POLLING_INTERVAL_MS);
        LOG.debug("{} - waiting to poll, interval={}", id, interval);
        Thread.sleep(interval);
      } catch (InterruptedException err) {
        LOG.info("{} - thread stopped with inconsistent state", id);
        this.stopped = true;
      } catch (Exception err) {
        LOG.error(id + " - thread interrupted", err);
        this.stopped = true;
      }
    }
  }

  /** Whether or not executor thread is stopped */
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void terminate() {
    this.stopped = true;
  }

  @Override
  public String toString() {
    return "Executor[" + id + "]";
  }
}
