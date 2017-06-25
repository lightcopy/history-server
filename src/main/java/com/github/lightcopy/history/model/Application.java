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

import org.apache.hadoop.fs.FileStatus;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.github.lightcopy.history.model.AbstractCodec;

/**
 * Class to represent Spark application and its metadata.
 */
public class Application extends AbstractCodec<Application> {
  // Processing status for event log instance
  public enum Status {
    PROCESSING, SUCCESS, FAILURE
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_APP_NAME = "appName";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_USER = "user";
  public static final String FIELD_IN_PROGRESS = "inProgress";
  public static final String FIELD_PATH = "path";
  public static final String FIELD_SIZE = "size";
  public static final String FIELD_MTIME = "mtime";
  public static final String FIELD_STATUS = "status";

  // application id (globally unique)
  private String appId;
  // application name
  private String appName;
  // application start time (as Spark app)
  private long starttime;
  // application end time (as Spark app)
  private long endtime;
  // user that launched application
  private String user;

  // == File statistics ==
  // whether or not application is in progress
  private boolean inProgress;
  // full path to the file
  private String path;
  // file size in bytes
  private long size;
  // modification time as timestamp in milliseconds
  private long mtime;
  // processing status
  private Status status;

  // default constructor for encode/decode
  public Application() {
    this.appId = null;
    this.appName = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.user = null;
    this.inProgress = false;
    this.path = null;
    this.size = 0L;
    this.mtime = -1L;
    this.status = Status.PROCESSING;
  }

  // == getters ==

  /** Get application id */
  public String getAppId() {
    return appId;
  }

  /** Get application name */
  public String getAppName() {
    return appName;
  }

  /** Get start time */
  public long getStartTime() {
    return starttime;
  }

  /** Get end time */
  public long getEndTime() {
    return endtime;
  }

  /** Get application user */
  public String getUser() {
    return user;
  }

  /** Return true, if current application is in progress, false otherwise */
  public boolean inProgress() {
    return inProgress;
  }

  /** Get processing status */
  public Status getStatus() {
    return status;
  }

  /** Get path to the event log */
  public String getPath() {
    return path;
  }

  /** Get size in bytes for event log */
  public long getSize() {
    return size;
  }

  /** Get modification time */
  public long getModificationTime() {
    return mtime;
  }

  // == setters ==

  public void setAppId(String id) {
    this.appId = id;
  }

  public void setAppName(String name) {
    this.appName = name;
  }

  public void setStartTime(long timestamp) {
    this.starttime = timestamp;
  }

  public void setEndTime(long timestamp) {
    this.endtime = timestamp;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setInProgress(boolean value) {
    this.inProgress = value;
  }

  public void setPath(String value) {
    this.path = value;
  }

  public void setSize(long value) {
    this.size = value;
  }

  public void setModificationTime(long value) {
    this.mtime = value;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  /**
   * Update processing status for current application.
   * Has side effect of setting modification time to current system time.
   * @param newStatus new status
   */
  public synchronized void updateStatus(Status newStatus) {
    status = newStatus;
    mtime = System.currentTimeMillis();
  }

  /**
   * Copy all data from other application into this one.
   * @param other application to copy data from
   */
  public synchronized void copyFrom(Application other) {
    setAppId(other.getAppId());
    setAppName(other.getAppName());
    setStartTime(other.getStartTime());
    setEndTime(other.getEndTime());
    setUser(other.getUser());
    setInProgress(other.inProgress());
    setPath(other.getPath());
    setSize(other.getSize());
    setModificationTime(other.getModificationTime());
    setStatus(other.getStatus());
  }

  /**
   * Create application instance from org.apache.hadoop.fs.FileStatus.
   * Only some properties that relate to event log are set.
   * If hadoop FileStatus is not a file, exception is thrown.
   *
   * @param fileStatus FileStatus instance
   * @return Application instance
   */
  public static Application fromStatus(FileStatus fileStatus) {
    if (!fileStatus.isFile()) {
      throw new IllegalArgumentException("Cannot create application from non-file: " + fileStatus);
    }
    // app id follows pattern:
    // app-YYYYMMDDHHmmss-SSSS[.inprogress] or local-1497733035840[.inprogress]
    String name = fileStatus.getPath().getName();
    boolean inProgress = name.endsWith(".inprogress");
    String appId = inProgress ? name.substring(0, name.lastIndexOf(".inprogress")) : name;
    Application app = new Application();
    app.setAppId(appId);
    app.setInProgress(inProgress);
    app.setPath(fileStatus.getPath().toString());
    app.setSize(fileStatus.getLen());
    app.setModificationTime(fileStatus.getModificationTime());
    app.setStatus(Status.PROCESSING);
    return app;
  }

  @Override
  public String toString() {
    // we only display attributes that processes rely on to make decision on scheduling app
    // plus, app name to see if application has been updated in map
    return getClass().getSimpleName() +
      "(appId=" + appId + ", appName=" + appName + ", mtime=" + mtime + ", status=" + status + ")";
  }

  // == Codec methods ==

  @Override
  public Application decode(BsonReader reader, DecoderContext decoderContext) {
    Application app = new Application();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          app.setAppId(safeReadString(reader));
          break;
        case FIELD_APP_NAME:
          app.setAppName(safeReadString(reader));
          break;
        case FIELD_STARTTIME:
          app.setStartTime(reader.readInt64());
          break;
        case FIELD_ENDTIME:
          app.setEndTime(reader.readInt64());
          break;
        case FIELD_USER:
          app.setUser(safeReadString(reader));
          break;
        case FIELD_IN_PROGRESS:
          app.setInProgress(reader.readBoolean());
          break;
        case FIELD_PATH:
          app.setPath(safeReadString(reader));
          break;
        case FIELD_SIZE:
          app.setSize(reader.readInt64());
          break;
        case FIELD_MTIME:
          app.setModificationTime(reader.readInt64());
          break;
        case FIELD_STATUS:
          app.setStatus(Status.valueOf(safeReadString(reader)));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return app;
  }

  @Override
  public Class<Application> getEncoderClass() {
    return Application.class;
  }

  @Override
  public void encode(BsonWriter writer, Application value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    safeWriteString(writer, FIELD_APP_NAME, value.getAppName());
    writer.writeInt64(FIELD_STARTTIME, value.getStartTime());
    writer.writeInt64(FIELD_ENDTIME, value.getEndTime());
    safeWriteString(writer, FIELD_USER, value.getUser());
    writer.writeBoolean(FIELD_IN_PROGRESS, value.inProgress());
    safeWriteString(writer, FIELD_PATH, value.getPath());
    writer.writeInt64(FIELD_SIZE, value.getSize());
    writer.writeInt64(FIELD_MTIME, value.getModificationTime());
    safeWriteString(writer, FIELD_STATUS, value.getStatus().name());
    writer.writeEndDocument();
  }
}
