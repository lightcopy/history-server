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

import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lightcopy.history.model.Metrics;
import com.github.lightcopy.history.model.Stage;

/**
 * Aggregated metrics for application.
 * Keeps track of all tasks, stages, jobs, and executors and their metrics, and ensures consistent
 * updates.
 */
public class ApplicationSummary {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationSummary.class);

  // stage data within application
  private HashMap<Long, StageMetrics> stages;
  private HashSet<Long> pendingStages;
  private HashSet<Long> activeStages;
  private HashSet<Long> completedStages;
  private HashSet<Long> failedStages;
  private HashSet<Long> skippedStages;

  // Stage methods, increment/decrement/shift

  /** Convert stage id and attempt number into unique stage identifier */
  private static long stageId(int stageId, int attempt) {
    return ((long) stageId) << 32 | attempt;
  }

  /** Method removes unique stage id from all status sets */
  private void unlinkStage(long id) {
    pendingStages.remove(id);
    activeStages.remove(id);
    completedStages.remove(id);
    failedStages.remove(id);
    skippedStages.remove(id);
  }

  /** Register stage in application summary, if stage already exists, just update status */
  public void upsertStage(int stageId, int attempt, Stage.Status status) {
    long id = stageId(stageId, attempt);
    if (!stages.containsKey(id)) {
      stages.put(id, new StageMetrics());
    }
    unlinkStage(id);
    switch (status) {
      case PENDING:
        pendingStages.add(id);
        break;
      case ACTIVE:
        activeStages.add(id);
        break;
      case COMPLETED:
        completedStages.add(id);
        break;
      case FAILED:
        failedStages.add(id);
        break;
      case SKIPPED:
        skippedStages.add(id);
        break;
      default:
        LOG.error("Ignore stage {} (attempt {}) with invalid status {}", stageId, attempt, status);
        break;
    }
  }

  public void incActiveTasks(int stageId, int attempt) {
    long id = stageId(stageId, attempt);
    if (activeStages.contains(id)) {
      stages.get(id).incActiveTasks();
    }
  }

  public void decActiveTasks(int stageId, int attempt) {
    long id = stageId(stageId, attempt);
    if (activeStages.contains(id)) {
      stages.get(id).decActiveTasks();
    }
  }

  public void incCompletedTasks(int stageId, int attempt) {
    long id = stageId(stageId, attempt);
    if (activeStages.contains(id)) {
      stages.get(id).incCompletedTasks();
    }
  }

  public void incFailedTasks(int stageId, int attempt) {
    long id = stageId(stageId, attempt);
    if (activeStages.contains(id)) {
      stages.get(id).incFailedTasks();
    }
  }

  public void incMetrics(int stageId, int attempt, Metrics metrics) {
    long id = stageId(stageId, attempt);
    if (activeStages.contains(id)) {
      stages.get(id).incMetrics(metrics);
    }
  }

  public void setTotalTasks(int stageId, int attempt, int totalTasks) {
    long id = stageId(stageId, attempt);
    stages.get(id).setTotalTasks(totalTasks);
  }

  public void markPending(int stageId, int attempt) {
    upsertStage(stageId, attempt, Stage.Status.PENDING);
  }

  public void markActive(int stageId, int attempt) {
    upsertStage(stageId, attempt, Stage.Status.ACTIVE);
  }

  public void markCompleted(int stageId, int attempt) {
    upsertStage(stageId, attempt, Stage.Status.COMPLETED);
  }

  public void markFailed(int stageId, int attempt) {
    upsertStage(stageId, attempt, Stage.Status.FAILED);
  }

  public void markSkipped(int stageId, int attempt) {
    upsertStage(stageId, attempt, Stage.Status.SKIPPED);
  }

  public int getActiveTasks(int stageId, int attempt) {
    return stages.get(stageId(stageId, attempt)).getActiveTasks();
  }

  public int getCompletedTasks(int stageId, int attempt) {
    return stages.get(stageId(stageId, attempt)).getCompletedTasks();
  }

  public int getFailedTasks(int stageId, int attempt) {
    return stages.get(stageId(stageId, attempt)).getFailedTasks();
  }

  public int getTotalTasks(int stageId, int attempt) {
    return stages.get(stageId(stageId, attempt)).getTotalTasks();
  }

  public Metrics getMetrics(int stageId, int attempt) {
    return stages.get(stageId(stageId, attempt)).getMetrics();
  }
}
