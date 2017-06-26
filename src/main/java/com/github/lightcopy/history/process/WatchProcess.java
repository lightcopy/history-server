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
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lightcopy.history.EventProcessException;
import com.github.lightcopy.history.model.Application;

/**
 * Watch process to list application files in root directory.
 *
 */
public class WatchProcess extends InterruptibleThread {
  private static final Logger LOG = LoggerFactory.getLogger(WatchProcess.class);
  // polling interval in milliseconds = 1.25 sec + random interval
  public static final int POLLING_INTERVAL_MS = 1250;
  // filter to apply when listing directory, [.inprogress] files are discarded
  private static final PathFilter LIST_FILTER = new PathFilter() {
    // pattern matches files: work/app-20170616163546-0000
    private final Pattern appPattern = Pattern.compile("^app-\\d{14}-\\d{4}$");
    // pattern matches files: work/local-1498349971751
    private final Pattern localPattern = Pattern.compile("^local-\\d{13}$");

    @Override
    public boolean accept(Path path) {
      return
        appPattern.matcher(path.getName()).matches() ||
        localPattern.matcher(path.getName()).matches();
    }
  };

  private final FileSystem fs;
  private final Path root;
  private final ConcurrentHashMap<String, Application> apps;
  private final BlockingQueue<Application> queue;
  private final Random rand;
  private volatile boolean stopped;

  public WatchProcess(FileSystem fs, Path root, ConcurrentHashMap<String, Application> apps,
      BlockingQueue<Application> queue) {
    this.fs = fs;
    this.root = root;
    this.apps = apps;
    this.queue = queue;
    this.rand = new Random();
    this.stopped = false;
  }

  @Override
  public void run() {
    while (!this.stopped) {
      try {
        // check out filter to see what files get discarded
        FileStatus[] statuses = fs.listStatus(this.root, LIST_FILTER);
        if (statuses != null && statuses.length > 0) {
          LOG.debug("Found {} statuses by listing directory", statuses.length);
          for (FileStatus status : statuses) {
            if (status.isFile()) {
              Application app = Application.fromFileStatus(status);
              LOG.debug("Found application log {}", app.getAppId());
              // we only schedule applications that are newly added or existing with failure
              // status and have been updated since.
              Application existingApp = apps.get(app.getAppId());
              if (existingApp == null) {
                // new application log - add to the map and queue
                LOG.info("Add new log {} for processing", app);
                apps.put(app.getAppId(), app);
                queue.put(app);
              } else if (existingApp.getLoadStatus() == Application.LoadStatus.LOAD_FAILURE &&
                  existingApp.getModificationTime() < app.getModificationTime()) {
                // check status of the log, if status if failure, but modification time is
                // greater than the one existing log has, update status and add to processing
                LOG.info("Reprocess existing log {} => {}", existingApp, app);
                apps.replace(app.getAppId(), app);
                queue.put(app);
              } else {
                LOG.debug("Discard already processed log {}", existingApp);
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
