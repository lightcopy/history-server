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

import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class SparkListenerExecutorAdded {
  public class ExecutorInfo {
    @SerializedName("Host") public String host;
    @SerializedName("Total Cores") public int totalCores;
    // contains keys "stdout" and "stderr"
    @SerializedName("Log Urls") public Map<String, String> logUrls;
  }

  @SerializedName("Timestamp") public long timestamp;
  @SerializedName("Executor ID") public String executorId;
  @SerializedName("Executor Info") public ExecutorInfo info;
}
