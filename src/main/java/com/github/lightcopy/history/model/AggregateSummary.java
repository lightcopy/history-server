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

/**
 * Class to keep track for aggregated metrics for stages/jobs/executors within application.
 * Not thread-safe.
 */
public class AggregateSummary {
  private AggregateSummary() {
  }

  /**
   * Track aggregate summary for stages within application.
   * @return StageAggregateTracker instance
   */
  public static StageAggregateTracker stages() {
    return new StageAggregateTracker();
  }

  /**
   * Track aggregate summary for jobs within application.
   * @return JobAggregateTracker instance
   */
  public static JobAggregateTracker jobs() {
    return new JobAggregateTracker();
  }

  static class AggregateTracker {
    // Map of Aggregate to keep track of aggregates, such as active/completed/failed tasks and
    // metrics; completed or failed jobs/stages are evicted from map, since no update is expected
    private HashMap<Long, Aggregate> map;

    /** Get or create new aggregate for provided id */
    private Aggregate getOrCreate(long id) {
      Aggregate agg = map.get(id);
      if (agg == null) {
        agg = new Aggregate();
        map.put(id, agg);
      }
      return agg;
    }

    protected AggregateTracker() {
      this.map = new HashMap<Long, Aggregate>();
    }

    /** Increment active tasks */
    protected void incActiveTasks(long id) {
      getOrCreate(id).incActiveTasks();
    }

    /** Decrement active tasks */
    protected void decActiveTasks(long id) {
      getOrCreate(id).decActiveTasks();
    }

    /** Increment completed tasks */
    protected void incCompletedTasks(long id) {
      getOrCreate(id).incCompletedTasks();
    }

    /** Increment failed tasks */
    protected void incFailedTasks(long id) {
      getOrCreate(id).incFailedTasks();
    }

    /** Update metrics */
    protected void updateMetrics(long id, Metrics update) {
      getOrCreate(id).updateMetrics(update);
    }

    /** Evict stage and/or stage attempt */
    protected void evict(long id) {
      // if id is added again, we will create new aggregate for it
      // the logic of processing new aggregate (e.g. after stage/job is finished) should be handled
      // by client
      map.remove(id);
    }

    /** Get active tasks for stage */
    protected int getActiveTasks(long id) {
      return getOrCreate(id).getActiveTasks();
    }

    /** Get completed tasks for stage */
    protected int getCompletedTasks(long id) {
      return getOrCreate(id).getCompletedTasks();
    }

    /** Get failed tasks for stage */
    protected int getFailedTasks(long id) {
      return getOrCreate(id).getFailedTasks();
    }

    /** Get metrics for stage */
    protected Metrics getMetrics(long id) {
      return getOrCreate(id).getMetrics();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof AggregateTracker)) return false;
      if (!obj.getClass().equals(this.getClass())) return false;
      AggregateTracker that = (AggregateTracker) obj;
      return this.map.equals(that.map);
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + "(" + map + ")";
    }
  }

  /**
   * Aggregate tracker for stages.
   * Provides API to update data for stage id and attempt number.
   */
  public static class StageAggregateTracker extends AggregateTracker {
    StageAggregateTracker() {
      super();
    }

    /** Convert stage id and attempt number into unique id for aggregate */
    private static long id(int stageId, int stageAttemptId) {
      return ((long) stageId) << 32 | stageAttemptId;
    }

    public void incActiveTasks(int stageId, int stageAttemptId) {
      incActiveTasks(id(stageId, stageAttemptId));
    }

    public void decActiveTasks(int stageId, int stageAttemptId) {
      decActiveTasks(id(stageId, stageAttemptId));
    }

    public void incCompletedTasks(int stageId, int stageAttemptId) {
      incCompletedTasks(id(stageId, stageAttemptId));
    }

    public void incFailedTasks(int stageId, int stageAttemptId) {
      incFailedTasks(id(stageId, stageAttemptId));
    }

    public void updateMetrics(int stageId, int stageAttemptId, Metrics update) {
      updateMetrics(id(stageId, stageAttemptId), update);
    }

    public void evict(int stageId, int stageAttemptId) {
      evict(id(stageId, stageAttemptId));
    }

    public int getActiveTasks(int stageId, int stageAttemptId) {
      return getActiveTasks(id(stageId, stageAttemptId));
    }

    public int getCompletedTasks(int stageId, int stageAttemptId) {
      return getCompletedTasks(id(stageId, stageAttemptId));
    }

    public int getFailedTasks(int stageId, int stageAttemptId) {
      return getFailedTasks(id(stageId, stageAttemptId));
    }

    public Metrics getMetrics(int stageId, int stageAttemptId) {
      return getMetrics(id(stageId, stageAttemptId));
    }
  }

  /**
   * Aggregate tracker for jobs.
   * Provides API to update data for job id.
   */
  public static class JobAggregateTracker extends AggregateTracker {
    JobAggregateTracker() {
      super();
    }

    public void incActiveTasks(int jobId) {
      super.incActiveTasks(jobId);
    }

    public void decActiveTasks(int jobId) {
      super.decActiveTasks(jobId);
    }

    public void incCompletedTasks(int jobId) {
      super.incCompletedTasks(jobId);
    }

    public void incFailedTasks(int jobId) {
      super.incFailedTasks(jobId);
    }

    public void updateMetrics(int jobId, Metrics update) {
      super.updateMetrics(jobId, update);
    }

    public void evict(int jobId) {
      super.evict(jobId);
    }

    public int getActiveTasks(int jobId) {
      return super.getActiveTasks(jobId);
    }

    public int getCompletedTasks(int jobId) {
      return super.getCompletedTasks(jobId);
    }

    public int getFailedTasks(int jobId) {
      return super.getFailedTasks(jobId);
    }

    public Metrics getMetrics(int jobId) {
      return super.getMetrics(jobId);
    }
  }

  /**
   * Class to keep track of aggregate metrics.
   * Not thread-safe.
   */
  static class Aggregate {
    private int activeTasks;
    private int completedTasks;
    private int failedTasks;
    private Metrics metrics;

    public Aggregate() {
      this.activeTasks = 0;
      this.completedTasks = 0;
      this.failedTasks = 0;
      this.metrics = new Metrics();
    }

    /** Increment number of active tasks */
    public void incActiveTasks() {
      this.activeTasks++;
    }

    /** Decrement active tasks */
    public void decActiveTasks() {
      if (this.activeTasks == 0) return;
      this.activeTasks--;
    }

    /** Increment number of completed tasks */
    public void incCompletedTasks() {
      this.completedTasks++;
    }

    /** Increment number of failed tasks */
    public void incFailedTasks() {
      this.failedTasks++;
    }

    /** Update stage metrics */
    public void updateMetrics(Metrics update) {
      this.metrics.merge(update);
    }

    /** Get active tasks */
    public int getActiveTasks() {
      return activeTasks;
    }

    /** Get completed tasks */
    public int getCompletedTasks() {
      return completedTasks;
    }

    /** Get failed tasks */
    public int getFailedTasks() {
      return failedTasks;
    }

    /** Get metrics */
    public Metrics getMetrics() {
      return metrics;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Aggregate)) return false;
      Aggregate agg = (Aggregate) obj;
      return this.activeTasks == agg.activeTasks &&
        this.completedTasks == agg.completedTasks &&
        this.failedTasks == agg.failedTasks &&
        this.metrics.equals(agg.metrics);
    }

    @Override
    public String toString() {
      return "Aggregate(" +
        "activeTasks=" + activeTasks +
        ", completedTasks=" + completedTasks +
        ", failedTasks=" + failedTasks +
        ", metrics=" + metrics +
        ")";
    }
  }
}
