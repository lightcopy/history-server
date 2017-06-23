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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

import com.github.lightcopy.history.event.Event;
import com.github.lightcopy.history.event.SparkListenerApplicationStart;
import com.github.lightcopy.history.event.SparkListenerApplicationEnd;
import com.github.lightcopy.history.event.SparkListenerEnvironmentUpdate;
import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.EventLog;

/**
 * Parser for Spark listener events. Also performs aggregation for metrics.
 * Created per event log.
 */
class EventParser {
  private static final Logger LOG = LoggerFactory.getLogger(EventParser.class);

  private Gson gson;

  EventParser() {
    this.gson = new Gson();
  }

  /**
   * Parse event logs from file.
   * @param fs file system
   * @param client Mongo client
   * @param log event log to parse
   */
  public void parseEventLog(FileSystem fs, MongoClient client, EventLog log)
      throws EventProcessException {
    FSDataInputStream in = null;
    try {
      in = fs.open(log.getPath());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String json;
      while ((json = reader.readLine()) != null) {
        Event event = gson.fromJson(json, Event.class);
        if (event.getEventName() != null) {
          parseJsonEvent(log.getAppId(), event, json, client);
        } else {
          LOG.warn("Drop event {} for app {}", json, log.getAppId());
        }
      }
      Thread.sleep(1000L);
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
    // block for upsert
    Mongo.UpsertBlock<Application> block = null;

    switch (event.getEventName()) {
      case "SparkListenerApplicationStart":
        final SparkListenerApplicationStart start =
          gson.fromJson(json, SparkListenerApplicationStart.class);
        // check that app id is the same as event log id
        if (!appId.equals(start.appId)) {
          throw new RuntimeException("App ID mismatch: " + appId + " != " + start.appId);
        }

        block = new Mongo.UpsertBlock<Application>() {
          @Override
          public Application update(Application app) {
            if (app == null) {
              app = new Application();
            }
            // update application based on event
            app.setName(start.appName);
            app.setId(start.appId);
            app.setStartTime(start.timestamp);
            app.setUser(start.user);
            return app;
          }
        };
        Mongo.findOneAndUpsert(Mongo.applicationCollection(client),
          Filters.eq(Application.FIELD_APP_ID, appId), block);
        break;

      case "SparkListenerApplicationEnd":
        final SparkListenerApplicationEnd end =
          gson.fromJson(json, SparkListenerApplicationEnd.class);
        // block to update end time of the application
        block = new Mongo.UpsertBlock<Application>() {
          @Override
          public Application update(Application app) {
            if (app == null) {
              app = new Application();
            }
            app.setEndTime(end.timestamp);
            return app;
          }
        };
        Mongo.findOneAndUpsert(Mongo.applicationCollection(client),
          Filters.eq(Application.FIELD_APP_ID, appId), block);
        break;

      case "SparkListenerEnvironmentUpdate":
        final SparkListenerEnvironmentUpdate env =
          gson.fromJson(json, SparkListenerEnvironmentUpdate.class);
        // block to update environment information of the application
        block = new Mongo.UpsertBlock<Application>() {
          @Override
          public Application update(Application app) {
            if (app == null) {
              app = new Application();
            }
            app.setJvmInformation(env.jvmInformation);
            app.setSparkProperties(env.sparkProperties);
            app.setSystemProperties(env.systemProperties);
            app.setClasspathEntries(env.classpathEntries);
            return app;
          }
        };
        Mongo.findOneAndUpsert(Mongo.applicationCollection(client),
          Filters.eq(Application.FIELD_APP_ID, appId), block);
        break;

      default:
        LOG.warn("Unrecongnized event {} ", event);
        break;
    }
  }
}
