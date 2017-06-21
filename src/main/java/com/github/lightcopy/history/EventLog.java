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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Event log contains information about event log file with app id and modification time.
 * Used to identify uniquely each processed application. App id should match Spark application
 * app id.
 */
public class EventLog implements Codec<EventLog> {
  // Processing status for event log instance
  public enum Status {
    IN_PROGRESS, SUCCESS, FAILURE
  }

  // app id (will not match file name in case of in progress application)
  private String appId;
  // whether or not application is in progress
  private boolean inProgress;
  // full path to the file
  private Path path;
  // modification time as timestamp in milliseconds
  private long mtime;
  // processing status
  private Status status;

  private EventLog(String appId, boolean inProgress, Path path, long mtime, Status status) {
    this.appId = appId;
    this.inProgress = inProgress;
    this.path = path;
    this.mtime = mtime;
    this.status = status;
  }

  // empty constructor for codec init
  public EventLog() {
    /* no-op */
  }

  /** Get application id */
  public String getAppId() {
    return appId;
  }

  /** Get processing status */
  public Status getStatus() {
    return status;
  }

  /** Get path to the event log */
  public Path getPath() {
    return path;
  }

  /** Get modification time */
  public long getModificationTime() {
    return mtime;
  }

  /** Return true, if current application is in progress, false otherwise */
  public boolean inProgress() {
    return inProgress;
  }

  /**
   * Update status for current event log.
   * Has side effect of setting modification time to current system time.
   * @param newStatus new status
   */
  public synchronized void update(Status newStatus) {
    status = newStatus;
    mtime = System.currentTimeMillis();
  }

  /**
   * Create event log instance from org.apache.hadoop.fs.FileStatus.
   * If file status is not a file, exception is thrown.
   * @param fileStatus FileStatus instance
   * @return EventLog instance
   */
  public static EventLog fromStatus(FileStatus fileStatus) {
    if (!fileStatus.isFile()) {
      throw new IllegalArgumentException("Cannot create event log from non-file: " + fileStatus);
    }
    // app id follows pattern:
    // app-YYYYMMDDHHmmss-SSSS[.inprogress] or local-1497733035840[.inprogress]
    String name = fileStatus.getPath().getName();
    boolean inProgress = name.endsWith(".inprogress");
    String appId = inProgress ? name.substring(0, name.lastIndexOf(".inprogress")) : name;
    return new EventLog(appId, inProgress, fileStatus.getPath(), fileStatus.getModificationTime(),
      Status.IN_PROGRESS);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(appId=" + appId + ", inProgress=" + inProgress +
      ", path=" + path + ", mtime=" + mtime + ", status=" + status + ")";
  }

  @Override
  public EventLog decode(BsonReader reader, DecoderContext decoderContext) {
    reader.readStartDocument();
    String appId = reader.readString("appId");
    boolean inProgress = reader.readBoolean("inProgress");
    Path path = new Path(reader.readString("path"));
    long mtime = reader.readInt64("mtime");
    Status status = Status.valueOf(reader.readString("status"));
    reader.readEndDocument();
    return new EventLog(appId, inProgress, path, mtime, status);
  }

  @Override
  public Class<EventLog> getEncoderClass() {
    return EventLog.class;
  }

  @Override
  public void encode(BsonWriter writer, EventLog value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    writer.writeString("appId", value.getAppId());
    writer.writeBoolean("inProgress", value.inProgress());
    writer.writeString("path", value.getPath().toString());
    writer.writeInt64("mtime", value.getModificationTime());
    writer.writeString("status", value.getStatus().name());
    writer.writeEndDocument();
  }
}
