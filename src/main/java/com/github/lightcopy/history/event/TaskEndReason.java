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

package com.github.lightcopy.history.event;

import com.google.gson.annotations.SerializedName;

public class TaskEndReason {
  // Success, FetchFailed, ExceptionFailure, TaskCommitDenied, TaskKilled, ExecutorLostFailure
  @SerializedName("Reason") public String reason;
  // fields for FetchFailed
  @SerializedName("Block Manager Address") public BlockManagerId blockManagerAddress;
  @SerializedName("Shuffle ID") public int shuffleId;
  @SerializedName("Map ID") public int mapId;
  @SerializedName("Reduce ID") public int reduceId;
  @SerializedName("Message") public String message;
  // fields for ExceptionFailure
  @SerializedName("Class Name") public String className;
  @SerializedName("Description") public String description;
  @SerializedName("Full Stack Trace") public String fullStackTrace;
  // fields for TaskCommitDenied
  @SerializedName("Job ID") public int jobId;
  @SerializedName("Partition ID") public int partitionId;
  @SerializedName("Attempt Number") public int attemptNumber;
  // fields for ExecutorLostFailure
  @SerializedName("Executor ID") public String executorId;
  @SerializedName("Exit Caused By App") public boolean causedByApp;
  @SerializedName("Loss Reason") public String lossReason;
  // fields for TaskKilled
  @SerializedName("Kill Reason") public String killReason;

  /** Whether or not task reason is Success */
  public boolean isSuccess() {
    return reason != null && reason.equals("Success");
  }

  /** Whether or not task was killed */
  public boolean isKilled() {
    return reason != null && reason.equals("TaskKilled");
  }

  public String getDescription() {
    // we mark reason that cannot be processed as "<unknown>"; this potentially indicates
    // bug in parsing and/or conversion, and should be fixed
    String msg = null;
    switch (reason) {
      case "Success":
        msg = "";
        break;
      case "FetchFailed":
        msg = "FetchFailed(" + blockManagerAddress + ", shuffleId=" + shuffleId + ", mapId=" +
          mapId + ", reduceId=" + reduceId + ", message=\n" + message + "\n)";
        break;
      case "ExceptionFailure":
        // full stack trace is available separately
        msg = className + ": " + description;
        break;
      case "TaskCommitDenied":
        msg = "TaskCommitDenied (Driver denied task commit) for job: " + jobId + ", partition: " +
          partitionId + ", attemptNumber: " + attemptNumber;
        break;
      case "TaskKilled":
        msg = "TaskKilled (" + ((killReason == null) ? "killed intentionally" : killReason) + ")";
        break;
      case "ExecutorLostFailure":
        String exitBehavior =
          (causedByApp) ? "caused by one of the running tasks" : "unrelated to the running tasks";
        msg = "ExecutorLostFailure (executor " + executorId + " exited " + exitBehavior +
          ") Reason: " + lossReason;
        break;
      case "TaskResultLost":
        msg = "TaskResultLost (result lost from block manager)";
        break;
      case "Resubmitted":
        msg = "Resubmitted (resubmitted due to lost executor)";
        break;
      default:
        msg = "Unknown reason";
        break;
    }
    return msg;
  }

  public String getDetails() {
    // details are only defined for exception failures
    return this.fullStackTrace;
  }
}
