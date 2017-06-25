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
import org.apache.hadoop.fs.Path;

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
import com.github.lightcopy.history.model.Environment;

/**
 * Parser for Spark listener events. Also performs aggregation for metrics.
 * Created per event log.
 */
public class EventParser {
  private static final Logger LOG = LoggerFactory.getLogger(EventParser.class);

  private Gson gson;

  public EventParser() {
    this.gson = new Gson();
  }

  /**
   * Parse application logs from file.
   * @param fs file system
   * @param client Mongo client
   * @param app application to parse (has partial data related to fs file)
   */
  public void parseApplicationLog(FileSystem fs, MongoClient client, Application app)
      throws EventProcessException {
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(app.getPath()));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String json;
      while ((json = reader.readLine()) != null) {
        Event event = gson.fromJson(json, Event.class);
        if (event.getEventName() != null) {
          parseJsonEvent(app.getAppId(), event, json, client);
        } else {
          LOG.warn("Drop event {} for app {}", json, app.getAppId());
        }
      }
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
  private void parseJsonEvent(final String appId, Event event, String json, MongoClient client) {
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

        Mongo.findOneAndUpsert(
          Mongo.applicationCollection(client),
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
              obj.setAppName(start.appName);
              obj.setStartTime(start.timestamp);
              obj.setUser(start.user);
              return obj;
            }
          }
        );
        break;

      case "SparkListenerApplicationEnd":
        final SparkListenerApplicationEnd end =
          gson.fromJson(json, SparkListenerApplicationEnd.class);
        Mongo.findOneAndUpsert(
          Mongo.applicationCollection(client),
          Filters.eq(Application.FIELD_APP_ID, appId),
          new Mongo.UpsertBlock<Application>() {
            @Override
            public Application update(Application obj) {
              if (obj == null) {
                obj = new Application();
                // update appId, because it is new application
                obj.setAppId(appId);
              }
              obj.setEndTime(end.timestamp);
              return obj;
            }
          }
        );
        break;

      case "SparkListenerEnvironmentUpdate":
        final SparkListenerEnvironmentUpdate env =
          gson.fromJson(json, SparkListenerEnvironmentUpdate.class);
        // block to update environment information of the application
        Mongo.findOneAndUpsert(
          Mongo.environmentCollection(client),
          Filters.eq(Environment.FIELD_APP_ID, appId),
          new Mongo.UpsertBlock<Environment>() {
            @Override
            public Environment update(Environment obj) {
              if (obj == null) {
                obj = new Environment();
                // update appId, because it is new application
                obj.setAppId(appId);
              }
              obj.setJvmInformation(env.jvmInformation);
              obj.setSparkProperties(env.sparkProperties);
              obj.setSystemProperties(env.systemProperties);
              obj.setClasspathEntries(env.classpathEntries);
              return obj;
            }
          }
        );
        break;

      default:
        LOG.warn("Unrecongnized event {} ", event);
        break;
    }
  }
}
