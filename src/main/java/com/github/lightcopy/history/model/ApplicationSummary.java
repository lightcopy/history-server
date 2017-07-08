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

import java.util.HashMap;
import java.util.HashSet;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.github.lightcopy.history.Mongo;

/**
 * Aggregated metrics for application.
 * Keeps track of all stages, jobs, and executors.
 */
public class ApplicationSummary extends AbstractCodec<ApplicationSummary> {
  /**
   * Job summary to keep track of stages per job.
   * This is done to avoid usage of counters in global summary that might result in wrong count
   * of non-zero attempts.
   */
  public static class JobSummary {
    public int jobId;
    // stage data within job
    public int pendingStages;
    public int activeStages;
    public int completedStages;
    public int failedStages;
    public int skippedStages;

    public JobSummary() {
      this.jobId = -1;
      this.pendingStages = 0;
      this.activeStages = 0;
      this.completedStages = 0;
      this.failedStages = 0;
      this.skippedStages = 0;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof JobSummary)) return false;
      JobSummary that = (JobSummary) obj;
      return this.jobId == that.jobId &&
        this.pendingStages == that.pendingStages &&
        this.activeStages == that.activeStages &&
        this.completedStages == that.completedStages &&
        this.failedStages == that.failedStages &&
        this.skippedStages == that.skippedStages;
    }

    @Override
    public String toString() {
      return "JobSummary(id=" + jobId +
        ", pendingStages=" + pendingStages +
        ", activeStages=" + activeStages +
        ", completedStages=" + completedStages +
        ", failedStages=" + failedStages +
        ", skippedStages=" + skippedStages +
        ")";
    }
  }

  public static final String FIELD_APP_ID = "appId";
  public static final String FIELD_RUNNING_JOBS = "runningJobs";
  public static final String FIELD_SUCCEEDED_JOBS = "succeededJobs";
  public static final String FIELD_FAILED_JOBS = "failedJobs";
  public static final String FIELD_ACTIVE_EXECUTORS = "activeExecutors";
  public static final String FIELD_REMOVED_EXECUTORS = "removedExecutors";
  public static final String FIELD_RUNNING_QUERIES = "runningQueries";
  public static final String FIELD_COMPLETED_QUERIES = "completedQueries";
  // job summary fields
  public static final String FIELD_JOB_ID = "jobId";
  public static final String FIELD_PENDING_STAGES = "pendingStages";
  public static final String FIELD_ACTIVE_STAGES = "activeStages";
  public static final String FIELD_COMPLETED_STAGES = "completedStages";
  public static final String FIELD_FAILED_STAGES = "failedStages";
  public static final String FIELD_SKIPPED_STAGES = "skippedStages";

  /** Read encoder for job summary */
  private static ReadEncoder<JobSummary> JOB_SUMMARY_READER = new ReadEncoder<JobSummary>() {
    @Override
    public JobSummary read(BsonReader reader) {
      JobSummary sum = new JobSummary();
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        switch (reader.readName()) {
          case FIELD_JOB_ID:
            sum.jobId = reader.readInt32();
            break;
          case FIELD_PENDING_STAGES:
            sum.pendingStages = reader.readInt32();
            break;
          case FIELD_ACTIVE_STAGES:
            sum.activeStages = reader.readInt32();
            break;
          case FIELD_COMPLETED_STAGES:
            sum.completedStages = reader.readInt32();
            break;
          case FIELD_FAILED_STAGES:
            sum.failedStages = reader.readInt32();
            break;
          case FIELD_SKIPPED_STAGES:
            sum.skippedStages = reader.readInt32();
            break;
          default:
            reader.skipValue();
            break;
        }
      }
      reader.readEndDocument();
      return sum;
    }
  };

  /** Write encoder for job summary */
  private static WriteEncoder<JobSummary> JOB_SUMMARY_WRITER = new WriteEncoder<JobSummary>() {
    @Override
    public void write(BsonWriter writer, JobSummary value) {
      writer.writeStartDocument();
      writer.writeInt32(FIELD_JOB_ID, value.jobId);
      writer.writeInt32(FIELD_PENDING_STAGES, value.pendingStages);
      writer.writeInt32(FIELD_ACTIVE_STAGES, value.activeStages);
      writer.writeInt32(FIELD_COMPLETED_STAGES, value.completedStages);
      writer.writeInt32(FIELD_FAILED_STAGES, value.failedStages);
      writer.writeInt32(FIELD_SKIPPED_STAGES, value.skippedStages);
      writer.writeEndDocument();
    }
  };

  private String appId;
  // job data within application
  // jobId is integer, but we store it as string in order to bypass Mongo conversion
  // TODO: add method to codec to convert HashMap<K, V> or HashMap<Integer, V>
  private HashMap<String, JobSummary> runningJobs;
  private HashMap<String, JobSummary> succeededJobs;
  private HashMap<String, JobSummary> failedJobs;
  // executor data within application
  private HashSet<String> activeExecutors;
  private HashSet<String> removedExecutors;
  // query data within application
  private int runningQueries;
  private int completedQueries;

  public ApplicationSummary() {
    this.appId = null;
    this.runningJobs = new HashMap<String, JobSummary>();
    this.succeededJobs = new HashMap<String, JobSummary>();
    this.failedJobs = new HashMap<String, JobSummary>();
    // we cannot keep just counts for executors, because there are more than 2 events for
    // adding and removing executors/block managers
    this.activeExecutors = new HashSet<String>();
    this.removedExecutors = new HashSet<String>();
    // queries have only 2 events: start and completion, we just use counters here
    this.runningQueries = 0;
    this.completedQueries = 0;
  }

  // == Getters ==

  public String getAppId() {
    return appId;
  }

  public HashMap<String, JobSummary> getRunningJobs() {
    return runningJobs;
  }

  public HashMap<String, JobSummary> getSucceededJobs() {
    return succeededJobs;
  }

  public HashMap<String, JobSummary> getFailedJobs() {
    return failedJobs;
  }

  public HashSet<String> getActiveExecutors() {
    return activeExecutors;
  }

  public HashSet<String> getRemovedExecutors() {
    return removedExecutors;
  }

  public int getRunningQueries() {
    return runningQueries;
  }

  public int getCompletedQueries() {
    return completedQueries;
  }

  // === Setters ==

  public void setAppId(String value) {
    this.appId = value;
  }

  public void setRunningJobs(HashMap<String, JobSummary> value) {
    this.runningJobs = value;
  }

  public void setSucceededJobs(HashMap<String, JobSummary> value) {
    this.succeededJobs = value;
  }

  public void setFailedJobs(HashMap<String, JobSummary> value) {
    this.failedJobs = value;
  }

  /** Update job summary */
  public void update(Job job) {
    String jobId = "" + job.getJobId();
    switch (job.getStatus()) {
      case RUNNING:
        upsertSummary(runningJobs, job);
        break;
      case SUCCEEDED:
        upsertSummary(succeededJobs, job);
        break;
      case FAILED:
        upsertSummary(failedJobs, job);
        break;
      default:
        // means that job had invalid status, e.g. UNKNOWN
        // we do not store such jobs in summary
        unlinkJob(jobId);
        break;
    }
  }

  /** Remove jobId from all maps */
  private void unlinkJob(String jobId) {
    this.runningJobs.remove(jobId);
    this.succeededJobs.remove(jobId);
    this.failedJobs.remove(jobId);
  }

  /** In-place update for Job summary based on provided job */
  private void upsertSummary(HashMap<String, JobSummary> summary, Job job) {
    String jobId = "" + job.getJobId();
    JobSummary jobSummary = summary.get(jobId);
    if (jobSummary == null) {
      unlinkJob(jobId);
      jobSummary = new JobSummary();
      summary.put(jobId, jobSummary);
    }
    // overwrite summary state
    jobSummary.jobId = job.getJobId();
    jobSummary.pendingStages = job.getPendingStages().size();
    jobSummary.activeStages = job.getActiveStages().size();
    jobSummary.completedStages = job.getCompletedStages().size();
    jobSummary.failedStages = job.getFailedStages().size();
    jobSummary.skippedStages = job.getSkippedStages().size();
  }

  public void setActiveExecutors(HashSet<String> value) {
    this.activeExecutors = value;
  }

  public void setRemovedExecutors(HashSet<String> value) {
    this.removedExecutors = value;
  }

  /** Update executors sets for provided executor */
  public void update(Executor exc) {
    activeExecutors.remove(exc.getExecutorId());
    removedExecutors.remove(exc.getExecutorId());
    switch (exc.getStatus()) {
      case ACTIVE:
        activeExecutors.add(exc.getExecutorId());
        break;
      case REMOVED:
        removedExecutors.add(exc.getExecutorId());
        break;
      default:
        // do not add to sets for unknown status
        break;
    }
  }

  public void setRunningQueries(int value) {
    this.runningQueries = value;
  }

  public void setCompletedQueries(int value) {
    this.completedQueries = value;
  }

  /** Update SQLExecution queries summary */
  public void update(SQLExecution query) {
    switch (query.getStatus()) {
      case RUNNING:
        runningQueries++;
        break;
      case COMPLETED:
        runningQueries--;
        completedQueries++;
        break;
      default:
        // no-op for unknown status
        break;
    }
  }

  // == Codec methods ==

  @Override
  public ApplicationSummary decode(BsonReader reader, DecoderContext decoderContext) {
    ApplicationSummary sum = new ApplicationSummary();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case FIELD_APP_ID:
          sum.setAppId(safeReadString(reader));
          break;
        case FIELD_RUNNING_JOBS:
          sum.setRunningJobs(readMap(reader, JOB_SUMMARY_READER));
          break;
        case FIELD_SUCCEEDED_JOBS:
          sum.setSucceededJobs(readMap(reader, JOB_SUMMARY_READER));
          break;
        case FIELD_FAILED_JOBS:
          sum.setFailedJobs(readMap(reader, JOB_SUMMARY_READER));
          break;
        case FIELD_ACTIVE_EXECUTORS:
          sum.setActiveExecutors(readSet(reader, STRING_ENCODER));
          break;
        case FIELD_REMOVED_EXECUTORS:
          sum.setRemovedExecutors(readSet(reader, STRING_ENCODER));
          break;
        case FIELD_RUNNING_QUERIES:
          sum.setRunningQueries(reader.readInt32());
          break;
        case FIELD_COMPLETED_QUERIES:
          sum.setCompletedQueries(reader.readInt32());
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    return sum;
  }

  @Override
  public Class<ApplicationSummary> getEncoderClass() {
    return ApplicationSummary.class;
  }

  @Override
  public void encode(BsonWriter writer, ApplicationSummary value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    safeWriteString(writer, FIELD_APP_ID, value.getAppId());
    writeMap(writer, FIELD_RUNNING_JOBS, value.getRunningJobs(), JOB_SUMMARY_WRITER);
    writeMap(writer, FIELD_SUCCEEDED_JOBS, value.getSucceededJobs(), JOB_SUMMARY_WRITER);
    writeMap(writer, FIELD_FAILED_JOBS, value.getFailedJobs(), JOB_SUMMARY_WRITER);
    writeSet(writer, FIELD_ACTIVE_EXECUTORS, value.getActiveExecutors(), STRING_ENCODER);
    writeSet(writer, FIELD_REMOVED_EXECUTORS, value.getRemovedExecutors(), STRING_ENCODER);
    writer.writeInt32(FIELD_RUNNING_QUERIES, value.getRunningQueries());
    writer.writeInt32(FIELD_COMPLETED_QUERIES, value.getCompletedQueries());
    writer.writeEndDocument();
  }

  // == Mongo methods ==

  public static ApplicationSummary getOrCreate(MongoClient client, String appId) {
    ApplicationSummary sum = Mongo.appSummary(client).find(Filters.eq(FIELD_APP_ID, appId)).first();
    if (sum == null) {
      sum = new ApplicationSummary();
      sum.setAppId(appId);
    }
    sum.setMongoClient(client);
    return sum;
  }

  @Override
  protected void upsert(MongoClient client) {
    if (appId == null) return;
    Mongo.upsertOne(
      Mongo.appSummary(client),
      Filters.eq(FIELD_APP_ID, appId),
      this
    );
  }
}
