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

import com.github.lightcopy.history.EventParser;
import com.github.lightcopy.history.EventProcessException;
import com.github.lightcopy.history.Mongo;
import com.github.lightcopy.history.model.Application;

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
  private BlockingQueue<Application> queue;
  private final Random rand;
  private volatile boolean stopped;

  public ExecutorProcess(int id, FileSystem fs, MongoClient mongo,
      BlockingQueue<Application> queue) {
    this.id = id;
    this.fs = fs;
    this.mongo = mongo;
    this.queue = queue;
    this.rand = new Random();
    this.stopped = false;
  }

  @Override
  public void run() {
    Application app;
    while (!this.stopped) {
      try {
        while ((app = queue.poll(POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS)) != null) {
          Application.LoadStatus currentStatus = app.getLoadStatus();
          // executor only runs event logs that are marked as PROCESSING
          if (currentStatus != Application.LoadStatus.LOAD_PROGRESS) {
            throw new RuntimeException("Scheduled application log " + app +
              " is not marked for progress (was " + currentStatus + ")");
          }
          LOG.debug("{} - prepare state for {}", id, app);
          // always clean up data before processing new app id
          // since we can end up with partial if log was in progress before shutdown or failed to
          // process
          Mongo.removeData(mongo, app.getAppId());
          // initial insert of the processing application log
          Mongo.applications(mongo).insertOne(app);

          LOG.info("{} - processing {}", id, app);
          try {
            EventParser parser = new EventParser();
            parser.parseApplicationLog(fs, mongo, app);
            currentStatus = Application.LoadStatus.LOAD_SUCCESS;
          } catch (EventProcessException err) {
            LOG.error("Failed to process application log " + app, err);
            currentStatus = Application.LoadStatus.LOAD_FAILURE;
          } finally {
            // upsert application log status
            updateApplication(mongo, app, currentStatus);
            LOG.info("Updated application log {}", app);
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

  /**
   * Update application in database with provided status.
   * Has a side effect of updating current application with latest changes from Mongo.
   */
  private void updateApplication(MongoClient client, final Application app,
      final Application.LoadStatus currentStatus) {
    // upsert application log status
    Mongo.findOneAndUpsert(
      Mongo.applications(mongo),
      Filters.eq(Application.FIELD_APP_ID, app.getAppId()),
      new Mongo.UpsertBlock<Application>() {
        @Override
        public Application update(Application obj) {
          // here we need to update application in Mongo to assign status
          // we assume that Mongo always has the latest updates
          obj.updateLoadStatus(currentStatus);
          app.copyFrom(obj);
          return obj;
        }
      }
    );
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
