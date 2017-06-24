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

package com.github.lightcopy.history.model;

/**
 * Application information extended with event log data.
 * Used for REST only, not stored in database.
 */
public class ApplicationLog {
  // application data
  private String name;
  private String id;
  private long starttime;
  private long endtime;
  private String user;

  // event log data
  // file path
  private String filepath;
  // file size in bytes
  private long size;
  // modification time as timestamp in milliseconds
  private long mtime;
  // processing status
  private EventLog.Status status;

  public ApplicationLog(Application app, EventLog log) {
    this.name = app.getName();
    this.id = app.getId();
    this.starttime = app.getStartTime();
    this.endtime = app.getEndTime();
    this.user = app.getUser();

    // full file path to the log file
    this.filepath = (log.getPath() == null) ? null : log.getPath().toString();
    // size in bytes
    this.size = log.getSize();
    // file modification time in milliseconds
    this.mtime = log.getModificationTime();
    // processing status
    this.status = log.getStatus();
  }

  /** Direct constructor, used for testing */
  public ApplicationLog(
      String name, String id, long start, long end, String user,
      String path, long size, long mtime, EventLog.Status status) {
    this.name = name;
    this.id = id;
    this.starttime = start;
    this.endtime = end;
    this.user = user;
    this.filepath = path;
    this.size = size;
    this.mtime = mtime;
    this.status = status;
  }

  /** Get application name */
  public String getName() {
    return name;
  }

  /** Get application id */
  public String getId() {
    return id;
  }

  /** Get application start time */
  public long getStartTime() {
    return starttime;
  }

  /** Get application end time */
  public long getEndTime() {
    return endtime;
  }

  /** Get application user */
  public String getUser() {
    return user;
  }

  /** Get log file path */
  public String getPath() {
    return filepath;
  }

  /** Get log file size */
  public long getSize() {
    return size;
  }

  /** Get log file modification time */
  public long getModificationTime() {
    return mtime;
  }

  /** Get log processing status */
  public EventLog.Status getStatus() {
    return status;
  }
}
