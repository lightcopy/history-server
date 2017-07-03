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

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class StageInfo {
  @SerializedName("Stage ID") public int stageId;
  @SerializedName("Stage Attempt ID") public int stageAttemptId;
  @SerializedName("Stage Name") public String stageName;
  @SerializedName("Number of Tasks") public int numTasks;
  @SerializedName("Parent IDs") public List<Integer> parentIds;
  @SerializedName("Details") public String details;
  @SerializedName("Submission Time") public long submissionTime;
  @SerializedName("Completion Time") public long completionTime;
  @SerializedName("Failure Reason") public String failureReason;

  public String getErrorDescription() {
    // return first line as description, if available
    if (failureReason != null) {
      int maxChars = 200;
      int index = failureReason.indexOf('\n');
      if (index > maxChars) {
        return failureReason.substring(0, maxChars).trim() + "...";
      } else {
        return failureReason.substring(0, index).trim();
      }
    }
    return "";
  }

  public String getErrorDetails() {
    return failureReason;
  }
}
