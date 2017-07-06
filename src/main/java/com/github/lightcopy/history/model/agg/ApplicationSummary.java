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

import com.github.lightcopy.history.model.Job;
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
  private HashMap<Integer, HashSet<Long>> stageToAttempt;
  private HashSet<Long> pendingStages;
  private HashSet<Long> activeStages;
  private HashSet<Long> completedStages;
  private HashSet<Long> failedStages;
  private HashSet<Long> skippedStages;
  // job data within application
  private HashMap<Integer, HashSet<Integer>> jobs;
  private HashSet<Integer> runningJobs;
  private HashSet<Integer> succeededJobs;
  private HashSet<Integer> failedJobs;

  public ApplicationSummary() {
    stages = new HashMap<Long, StageMetrics>();
    stageToAttempt = new HashMap<Integer, HashSet<Long>>();
    pendingStages = new HashSet<Long>();
    activeStages = new HashSet<Long>();
    completedStages = new HashSet<Long>();
    failedStages = new HashSet<Long>();
    skippedStages = new HashSet<Long>();
    jobs = new HashMap<Integer, HashSet<Integer>>();
    runningJobs = new HashSet<Integer>();
    succeededJobs = new HashSet<Integer>();
    failedJobs = new HashSet<Integer>();
  }

  // == Stage methods, increment/decrement/shift ==

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
      if (!stageToAttempt.containsKey(stageId)) {
        stageToAttempt.put(stageId, new HashSet<Long>());
      }
      stageToAttempt.get(stageId).add(id);
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

  public Metrics getMetrics(int stageId, int attempt) {
    return stages.get(stageId(stageId, attempt)).getMetrics();
  }

  // == Job updates ==

  /** Method removes job id from all status sets */
  private void unlinkJob(int id) {
    runningJobs.remove(id);
    succeededJobs.remove(id);
    failedJobs.remove(id);
  }

  /** Link stage (regardless of attempt) to the job */
  public void addStageToJob(int jobId, int stageId) {
    if (!jobs.containsKey(jobId)) {
      jobs.put(jobId, new HashSet<Integer>());
    }
    jobs.get(jobId).add(stageId);
  }

  /** Register job in application summary, if job already exists, just update status */
  public void upsertJob(int jobId, Job.Status status) {
    if (!jobs.containsKey(jobId)) {
      jobs.put(jobId, new HashSet<Integer>());
    }
    unlinkJob(jobId);
    switch (status) {
      case RUNNING:
        runningJobs.add(jobId);
        break;
      case SUCCEEDED:
        succeededJobs.add(jobId);
        break;
      case FAILED:
        failedJobs.add(jobId);
        break;
      default:
        LOG.error("Ignore job {} with invalid status {}", jobId, status);
        break;
    }
  }

  public void markJobRunning(int jobId) {
    upsertJob(jobId, Job.Status.RUNNING);
  }

  public void markJobSucceeded(int jobId) {
    upsertJob(jobId, Job.Status.SUCCEEDED);
  }

  public void markJobFailed(int jobId) {
    upsertJob(jobId, Job.Status.FAILED);
  }

  /** All active tasks in all stages for job */
  public int getActiveTasks(int jobId) {
    int numTasks = 0;
    for (int stage : jobs.get(jobId)) {
      for (long uniqueId : stageToAttempt.get(stage)) {
        numTasks += stages.get(uniqueId).getActiveTasks();
      }
    }
    return numTasks;
  }

  public int getCompletedTasks(int jobId) {
    int numTasks = 0;
    for (int stage : jobs.get(jobId)) {
      for (long uniqueId : stageToAttempt.get(stage)) {
        numTasks += stages.get(uniqueId).getCompletedTasks();
      }
    }
    return numTasks;
  }

  public int getFailedTasks(int jobId) {
    int numTasks = 0;
    for (int stage : jobs.get(jobId)) {
      for (long uniqueId : stageToAttempt.get(stage)) {
        numTasks += stages.get(uniqueId).getFailedTasks();
      }
    }
    return numTasks;
  }

  // method to return number of stages that appear in provided stage set
  private int getStages(int jobId, HashSet<Long> stagesByStatus) {
    int numStages = 0;
    for (int stage : jobs.get(jobId)) {
      for (long uniqueId : stageToAttempt.get(stage)) {
        if (stagesByStatus.contains(uniqueId)) {
          numStages++;
        }
      }
    }
    return numStages;
  }

  public int getPendingStages(int jobId) {
    return getStages(jobId, pendingStages);
  }

  public int getActiveStages(int jobId) {
    return getStages(jobId, activeStages);
  }

  public int getCompletedStages(int jobId) {
    return getStages(jobId, completedStages);
  }

  public int getFailedStages(int jobId) {
    return getStages(jobId, failedStages);
  }

  public int getSkippedStages(int jobId) {
    return getStages(jobId, skippedStages);
  }

  public int getRunningJobs() {
    return runningJobs.size();
  }

  public int getSucceededJobs() {
    return succeededJobs.size();
  }

  public int getFailedJobs() {
    return failedJobs.size();
  }
}
