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

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.github.lightcopy.history.Mongo;

/**
 * Class to represent Spark application and its metadata.
 */
public class Application extends AbstractCodec<Application> {
  // Processing status for event log instance
  public enum LoadStatus {
    LOAD_PROGRESS, LOAD_SUCCESS, LOAD_FAILURE
  }

  // List of available Spark application statuses
  public enum AppStatus {
    NONE, IN_PROGRESS, FINISHED
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_APP_NAME = "appName";
  public static final String FIELD_STARTTIME = "starttime";
  public static final String FIELD_ENDTIME = "endtime";
  public static final String FIELD_USER = "user";
  public static final String FIELD_APP_STATUS = "appStatus";
  public static final String FIELD_PATH = "path";
  public static final String FIELD_SIZE = "size";
  public static final String FIELD_MTIME = "mtime";
  public static final String FIELD_LOAD_STATUS = "loadStatus";

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
  // application status
  private AppStatus appStatus;

  // == File statistics ==
  // full path to the file
  private String path;
  // file size in bytes
  private long size;
  // modification time as timestamp in milliseconds
  private long mtime;
  // processing status
  private LoadStatus loadStatus;

  // default constructor for encode/decode
  public Application() {
    this.appId = null;
    this.appName = null;
    this.starttime = -1L;
    this.endtime = -1L;
    this.user = null;
    this.appStatus = AppStatus.NONE;
    this.path = null;
    this.size = 0L;
    this.mtime = -1L;
    this.loadStatus = LoadStatus.LOAD_PROGRESS;
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

  /** Get application status */
  public AppStatus getAppStatus() {
    return appStatus;
  }

  /** Get processing (loading by history server) status */
  public LoadStatus getLoadStatus() {
    return loadStatus;
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

  public void setAppStatus(AppStatus status) {
    this.appStatus = status;
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

  public void setLoadStatus(LoadStatus status) {
    this.loadStatus = status;
  }

  /**
   * Update application status for current instance.
   * @param newStatus new application status
   */
  public synchronized void updateAppStatus(AppStatus newStatus) {
    appStatus = newStatus;
  }

  /**
   * Update processing status for current application.
   * Has side effect of setting modification time to current system time.
   * @param newStatus new status
   */
  public synchronized void updateLoadStatus(LoadStatus newStatus) {
    loadStatus = newStatus;
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
    setAppStatus(other.getAppStatus());
    setPath(other.getPath());
    setSize(other.getSize());
    setModificationTime(other.getModificationTime());
    setLoadStatus(other.getLoadStatus());
  }

  /**
   * Create application instance from org.apache.hadoop.fs.FileStatus.
   * Only some properties that relate to event log are set.
   * If hadoop FileStatus is not a file, exception is thrown.
   *
   * @param fileStatus FileStatus instance
   * @return Application instance
   */
  public static Application fromFileStatus(FileStatus fileStatus) {
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
    app.setAppStatus(AppStatus.NONE);
    app.setPath(fileStatus.getPath().toString());
    app.setSize(fileStatus.getLen());
    app.setModificationTime(fileStatus.getModificationTime());
    app.setLoadStatus(LoadStatus.LOAD_PROGRESS);
    return app;
  }

  @Override
  public String toString() {
    // we only display attributes that processes rely on to make decision on scheduling app
    // plus, app name to see if application has been updated in map
    return getClass().getSimpleName() +
      "(appId=" + appId +
      ", appName=" + appName +
      ", appStatus=" + appStatus +
      ", mtime=" + mtime +
      ", loadStatus=" + loadStatus + ")";
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
        case FIELD_APP_STATUS:
          app.setAppStatus(AppStatus.valueOf(safeReadString(reader)));
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
        case FIELD_LOAD_STATUS:
          app.setLoadStatus(LoadStatus.valueOf(safeReadString(reader)));
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
    safeWriteString(writer, FIELD_APP_STATUS, value.getAppStatus().name());
    safeWriteString(writer, FIELD_PATH, value.getPath());
    writer.writeInt64(FIELD_SIZE, value.getSize());
    writer.writeInt64(FIELD_MTIME, value.getModificationTime());
    safeWriteString(writer, FIELD_LOAD_STATUS, value.getLoadStatus().name());
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static Application getOrCreate(MongoClient client, String appId) {
    Application app = Mongo.applications(client).find(Filters.eq(FIELD_APP_ID, appId)).first();
    if (app == null) {
      app = new Application();
      app.setAppId(appId);
    }
    app.setMongoClient(client);
    return app;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null) return;
    Mongo.findAndUpsertOne(
      Mongo.applications(client),
      Filters.eq(FIELD_APP_ID, appId),
      this
    );
  }
}
