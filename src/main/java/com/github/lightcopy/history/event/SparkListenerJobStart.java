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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class SparkListenerJobStart {
  @SerializedName("Job ID") public int jobId;
  @SerializedName("Submission Time") public long submissionTime;
  @SerializedName("Stage Infos") public List<StageInfo> stageInfos;
  @SerializedName("Properties") public Map<String, String> properties;

  /** Get job name as the last stage name */
  public String getJobName() {
    // search for the stage with the largest id and use name as job name
    int stageId = -1;
    String name = "Job " + jobId + " (unknown)";
    for (StageInfo info : stageInfos) {
      if (info != null && stageId <= info.stageId && info.stageName != null) {
        stageId = info.stageId;
        name = info.stageName;
      }
    }
    return name;
  }

  /** Get total tasks based on stage infos */
  public int getTotalTasks() {
    // Compute (a potential underestimate of) the number of tasks that will be run by this job.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    int numTasks = 0;
    for (StageInfo info : stageInfos) {
      if (info != null && info.completionTime <= 0) {
        numTasks += info.numTasks;
      }
    }
    return numTasks;
  }

  /** Get query execution id or -1 if there is no link to sql execution */
  public int getExecutionId() {
    if (properties != null) {
      String query = properties.get("spark.sql.execution.id");
      if (query != null && !query.isEmpty()) {
        // do not fall back to the -1, if query fails to parse
        // this might indicate potential incompatibility between Spark versions
        return Integer.parseInt(query);
      }
    }
    // could not find query for the job
    return -1;
  }

  /** Get job group or null if no group is not provided */
  public String getJobGroup() {
    // if property is not found, it is okay to return null
    if (properties == null) return null;
    return properties.get("spark.jobGroup.id");
  }
}
