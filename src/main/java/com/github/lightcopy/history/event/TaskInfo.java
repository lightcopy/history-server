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

public class TaskInfo {
  @SerializedName("Task ID") public long taskId;
  @SerializedName("Index") public int index;
  @SerializedName("Attempt") public int attempt;
  @SerializedName("Launch Time") public long launchTime;
  @SerializedName("Executor ID") public String executorId;
  @SerializedName("Host") public String host;
  @SerializedName("Locality") public String locality;
  @SerializedName("Speculative") public boolean speculative;
  // The time when the task started remotely getting the result. Will not be set if the
  // task result was sent immediately when the task finished (as opposed to sending an
  // IndirectTaskResult and later fetching the result from the block manager).
  @SerializedName("Getting Result Time") public long gettingResultTime;
  // The time when the task has completed successfully (including the time to remotely fetch
  // results, if necessary).
  @SerializedName("Finish Time") public long finishTime;
  @SerializedName("Failed") public boolean failed;
  @SerializedName("Killed") public boolean killed;
  @SerializedName("Accumulables") public List<AccumulableInfo> accumulables;
}
