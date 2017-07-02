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
 * Class to keep track for aggregated metrics for stages within application.
 * Not thread-safe.
 */
public class StageAggregateTracker {
  // Map of stageAggregate to keep track of stage aggregates, such as active/completed/failed tasks
  // and metrics; completed or failed stages are evicted from map, since no update is expected
  private HashMap<Integer, HashMap<Integer, StageAggregate>> map;

  public StageAggregateTracker() {
    this.map = new HashMap<Integer, HashMap<Integer, StageAggregate>>();
  }

  private void createIfAbsent(int stageId, int stageAttemptId) {
    if (!map.containsKey(stageId)) {
      HashMap<Integer, StageAggregate> agg = new HashMap<Integer, StageAggregate>();
      map.put(stageId, agg);
    }
    if (!map.get(stageId).containsKey(stageAttemptId)) {
      map.get(stageId).put(stageAttemptId, new StageAggregate());
    }
  }

  /** Increment active tasks */
  public void incActiveTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    map.get(stageId).get(stageAttemptId).incActiveTasks();
  }

  /** Decrement active tasks */
  public void decActiveTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    map.get(stageId).get(stageAttemptId).decActiveTasks();
  }

  /** Increment completed tasks */
  public void incCompletedTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    map.get(stageId).get(stageAttemptId).incCompletedTasks();
  }

  /** Increment failed tasks */
  public void incFailedTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    map.get(stageId).get(stageAttemptId).incFailedTasks();
  }

  /** Update metrics */
  public void updateMetrics(int stageId, int stageAttemptId, Metrics update) {
    createIfAbsent(stageId, stageAttemptId);
    map.get(stageId).get(stageAttemptId).updateMetrics(update);
  }

  /** Evict stage and/or stage attempt */
  public void evict(int stageId, int stageAttemptId) {
    if (map.containsKey(stageId)) {
      map.get(stageId).remove(stageAttemptId);
      if (map.get(stageId).size() == 0) {
        map.remove(stageId);
      }
    }
  }

  /** Get active tasks for stage */
  public int getActiveTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    return map.get(stageId).get(stageAttemptId).getActiveTasks();
  }

  /** Get completed tasks for stage */
  public int getCompletedTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    return map.get(stageId).get(stageAttemptId).getCompletedTasks();
  }

  /** Get failed tasks for stage */
  public int getFailedTasks(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    return map.get(stageId).get(stageAttemptId).getFailedTasks();
  }

  /** Get metrics for stage */
  public Metrics getMetrics(int stageId, int stageAttemptId) {
    createIfAbsent(stageId, stageAttemptId);
    return map.get(stageId).get(stageAttemptId).getMetrics();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof StageAggregateTracker)) return false;
    StageAggregateTracker that = (StageAggregateTracker) obj;
    return this.map.equals(that.map);
  }

  @Override
  public String toString() {
    return "Tracker(" + map + ")";
  }

  /**
   * Class to keep track of aggregate metrics for a stage.
   * Not thread-safe.
   */
  static class StageAggregate {
    private int activeTasks;
    private int completedTasks;
    private int failedTasks;
    private Metrics metrics;

    public StageAggregate() {
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
      if (obj == null || !(obj instanceof StageAggregate)) return false;
      StageAggregate agg = (StageAggregate) obj;
      return this.activeTasks == agg.activeTasks &&
        this.completedTasks == agg.completedTasks &&
        this.failedTasks == agg.failedTasks &&
        this.metrics.equals(agg.metrics);
    }

    @Override
    public String toString() {
      return "StageAggregate(" +
        "activeTasks=" + activeTasks +
        ", completedTasks=" + completedTasks +
        ", failedTasks=" + failedTasks +
        ", metrics=" + metrics +
        ")";
    }
  }
}
