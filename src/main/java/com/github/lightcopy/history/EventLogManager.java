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
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process to list applications in event directory and schedule them for processing.
 */
class EventLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogManager.class);

  private FileSystem fs;
  private Path root;
  private WatchProcess watchProcess;
  private Thread watchProcessThread;

  public EventLogManager(FileSystem fs, String rootDirectory) {
    this.fs = fs;
    this.root = new Path(rootDirectory);
  }

  public void start() {
    LOG.info("Start event log manager");
    this.watchProcess = new WatchProcess(this.fs, this.root);
    this.watchProcessThread = new Thread(this.watchProcess);
    LOG.info("Start watch thread {}", this.watchProcessThread);
    this.watchProcessThread.start();
  }

  public void stop() {
    LOG.info("Stop event log manager");
    if (this.watchProcessThread != null) {
      try {
        this.watchProcess.terminate();
        this.watchProcessThread.join();
        LOG.info("Stopped watch process");
        // reset properties to null
        this.watchProcessThread = null;
        this.watchProcess = null;
      } catch (InterruptedException err) {
        throw new RuntimeException("Intrerrupted thread " + this.watchProcessThread, err);
      }
    }
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
    private final Random rand;
    private volatile boolean stopped;

    public WatchProcess(FileSystem fs, Path root) {
      this.fs = fs;
      this.rand = new Random();
      this.stopped = false;
    }

    @Override
    public void run() {
      while (!this.stopped) {
        try {
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
