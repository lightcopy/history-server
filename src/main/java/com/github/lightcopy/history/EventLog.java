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

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_IN_PROGRESS = "inProgress";
  public static final String FIELD_PATH = "path";
  public static final String FIELD_SIZE = "size";
  public static final String FIELD_MTIME = "mtime";
  public static final String FIELD_STATUS = "status";

  // app id (will not match file name in case of in progress application)
  private String appId;
  // whether or not application is in progress
  private boolean inProgress;
  // full path to the file
  private Path path;
  // file size in bytes
  private long sizeBytes;
  // modification time as timestamp in milliseconds
  private long mtime;
  // processing status
  private Status status;

  private EventLog(String appId, boolean inProgress, Path path, long sizeBytes, long mtime,
      Status status) {
    this.appId = appId;
    this.inProgress = inProgress;
    this.path = path;
    this.sizeBytes = sizeBytes;
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

  /** Get size in bytes for event log */
  public long getSize() {
    return sizeBytes;
  }

  /** Get modification time */
  public long getModificationTime() {
    return mtime;
  }

  /** Return true, if current application is in progress, false otherwise */
  public boolean inProgress() {
    return inProgress;
  }

  // == Setters for codec ==

  private void setAppId(String value) {
    this.appId = value;
  }

  private void setInProgress(boolean value) {
    this.inProgress = value;
  }

  private void setPath(Path value) {
    this.path = value;
  }

  private void setSize(long value) {
    this.sizeBytes = value;
  }

  private void setModificationTime(long value) {
    this.mtime = value;
  }

  private void setStatus(Status status) {
    this.status = status;
  }

  /**
   * Update status for current event log.
   * Has side effect of setting modification time to current system time.
   * @param newStatus new status
   */
  public synchronized void updateStatus(Status newStatus) {
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
    return new EventLog(appId, inProgress, fileStatus.getPath(), fileStatus.getLen(),
      fileStatus.getModificationTime(), Status.IN_PROGRESS);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(appId=" + appId + ", inProgress=" + inProgress +
      ", path=" + path + ", size=" + sizeBytes + ", mtime=" + mtime + ", status=" + status + ")";
  }

  @Override
  public EventLog decode(BsonReader reader, DecoderContext decoderContext) {
    EventLog log = new EventLog();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          log.setAppId(reader.readString());
          break;
        case FIELD_IN_PROGRESS:
          log.setInProgress(reader.readBoolean());
          break;
        case FIELD_PATH:
          log.setPath(new Path(reader.readString()));
          break;
        case FIELD_SIZE:
          log.setSize(reader.readInt64());
          break;
        case FIELD_MTIME:
          log.setModificationTime(reader.readInt64());
          break;
        case FIELD_STATUS:
          log.setStatus(Status.valueOf(reader.readString()));
          break;
        default:
          // ignore any other fields, e.g. object id
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return log;
  }

  @Override
  public Class<EventLog> getEncoderClass() {
    return EventLog.class;
  }

  @Override
  public void encode(BsonWriter writer, EventLog value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    writer.writeString(FIELD_APP_ID, value.getAppId());
    writer.writeBoolean(FIELD_IN_PROGRESS, value.inProgress());
    writer.writeString(FIELD_PATH, value.getPath().toString());
    writer.writeInt64(FIELD_SIZE, value.getSize());
    writer.writeInt64(FIELD_MTIME, value.getModificationTime());
    writer.writeString(FIELD_STATUS, value.getStatus().name());
    writer.writeEndDocument();
  }
}
