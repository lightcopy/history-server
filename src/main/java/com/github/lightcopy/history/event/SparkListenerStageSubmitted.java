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

public class SparkListenerStageSubmitted {
  @SerializedName("Stage Info") public StageInfo stageInfo;
  @SerializedName("Properties") public Map<String, String> properties;

  /** Get job group for stage or null if group is not provided */
  public String getJobGroup() {
    // if property is not found, it is okay to return null
    if (properties == null) return null;
    return properties.get("spark.jobGroup.id");
  }
}
