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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/** Simple development server for frontend */
public class Server extends AbstractServer {
  // file system for event logs
  private FileSystem fs;
  // resolved path to the root directory
  private String rootDirectory;
  // Mongo client
  private MongoClient mongo;
  // event log manager
  private EventLogManager eventLogManager;

  public Server() throws IOException {
    super();
    // initialize file system and check event log dir path
    // load configuration with any defaults available on machine
    Path path = new Path(this.conf.eventLogDir());
    this.fs = path.getFileSystem(new Configuration());
    // HDFS error is not explicit about directory, we rethrow exception with appropriate message
    if (!this.fs.exists(path)) {
      throw new IOException("Directory " + path + " does not exist");
    }
    FileStatus status = fs.getFileStatus(path);
    if (!status.isDirectory()) {
      throw new IOException("Path " + path + " is not a directory, got status: " + status);
    }
    this.rootDirectory = status.getPath().toString();
    LOG.info("Resolved event log directory as {}", this.rootDirectory);

    // shutdown of client is handled by server, classes should not close client connections
    this.mongo = new MongoClient(new MongoClientURI(this.conf.mongoConnectionString()));
    LOG.info("Created mongo client {}", this.mongo);

    this.eventLogManager = new EventLogManager(this.fs, this.rootDirectory, this.mongo);
    registerShutdownHook(new EventLogManagerShutdown(this.eventLogManager));
    registerShutdownHook(new MongoClientShutdown(this.mongo));
  }

  // shutdown hook for the event log manager
  static class EventLogManagerShutdown implements Runnable {
    private EventLogManager manager;

    EventLogManagerShutdown(EventLogManager manager) {
      this.manager = manager;
    }

    @Override
    public void run() {
      if (this.manager != null) {
        this.manager.stop();
        this.manager = null;
      }
    }
  }

  // shutdown hook for the mongo client
  static class MongoClientShutdown implements Runnable {
    private MongoClient client;

    MongoClientShutdown(MongoClient client) {
      this.client = client;
    }

    @Override
    public void run() {
      if (this.client != null) {
        this.client.close();
        this.client = null;
      }
    }
  }

  @Override
  public void afterLaunch() {
    // launch event listing process
    this.eventLogManager.start();
  }

  public static void main(String[] args) {
    try {
      LOG.info("Initialize web server");
      Server server = new Server();
      LOG.info("Created server {}", server);
      server.launch();
    } catch (Exception err) {
      LOG.error("Exception occurred", err);
      System.exit(1);
    }
  }
}
