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

package com.github.lightcopy.history.model.agg;

import com.github.lightcopy.history.model.Metrics;

/**
 * Aggregated metrics for stage.
 */
public class StageMetrics {
  private int activeTasks;
  private int completedTasks;
  private int failedTasks;
  private int totalTasks;
  private Metrics metrics;

  public StageMetrics() {
    this.activeTasks = 0;
    this.completedTasks = 0;
    this.failedTasks = 0;
    this.totalTasks = 0;
    this.metrics = new Metrics();
  }

  /** Increment active tasks by 1 */
  public void incActiveTasks() {
    activeTasks++;
  }

  /** Decrement active tasks by 1 */
  public void decActiveTasks() {
    activeTasks--;
  }

  /** Increment completed tasks by 1 */
  public void incCompletedTasks() {
    completedTasks++;
  }

  /** Increment failed tasks by 1 */
  public void incFailedTasks() {
    failedTasks++;
  }

  /** Set value of total tasks */
  public void setTotalTasks(int value) {
    totalTasks = value;
  }

  /** Update current metrics with delta update */
  public void incMetrics(Metrics update) {
    metrics.merge(update);
  }

  // == Getters ==

  /** Get active tasks for stage */
  public int getActiveTasks() {
    return activeTasks;
  }

  /** Get completed tasks for stage */
  public int getCompletedTasks() {
    return completedTasks;
  }

  /** Get failed tasks for stage */
  public int getFailedTasks() {
    return failedTasks;
  }

  /** Get total tasks for stage */
  public int getTotalTasks() {
    return totalTasks;
  }

  /** Get metrics for stage */
  public Metrics getMetrics() {
    return metrics;
  }
}
