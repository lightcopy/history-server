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

public class TaskMetrics {
  public static class ShuffleReadMetrics {
    @SerializedName("Remote Blocks Fetched") public long remoteBlocksFetched;
    @SerializedName("Local Blocks Fetched") public long localBlocksFetched;
    @SerializedName("Fetch Wait Time") public long fetchWaitTime;
    @SerializedName("Remote Bytes Read") public long remoteBytesRead;
    @SerializedName("Local Bytes Read") public long localBytesRead;
    @SerializedName("Total Records Read") public long totalRecordsRead;
  }

  public static class ShuffleWriteMetrics {
    @SerializedName("Shuffle Bytes Written") public long shuffleBytesWritten;
    @SerializedName("Shuffle Write Time") public long shuffleWriteTime;
    @SerializedName("Shuffle Records Written") public long shuffleRecordsWritten;
  }

  public static class InputMetrics {
    @SerializedName("Bytes Read") public long bytesRead;
    @SerializedName("Records Read") public long recordsRead;
  }

  public static class OutputMetrics {
    @SerializedName("Bytes Written") public long bytesWritten;
    @SerializedName("Records Written") public long recordsWritten;
  }

  @SerializedName("Executor Deserialize Time") public long executorDeserializeTime;
  @SerializedName("Executor Deserialize CPU Time") public long executorDeserializeCpuTime;
  @SerializedName("Executor Run Time") public long executorRunTime;
  @SerializedName("Executor CPU Time") public long executorCpuTime;
  @SerializedName("Result Size") public long resultSize;
  @SerializedName("JVM GC Time") public long jvmGcTime;
  @SerializedName("Result Serialization Time") public long resultSerializationTime;
  @SerializedName("Memory Bytes Spilled") public long memoryBytesSpilled;
  @SerializedName("Disk Bytes Spilled") public long diskBytesSpilled;
  @SerializedName("Shuffle Read Metrics") public ShuffleReadMetrics shuffleReadMetrics;
  @SerializedName("Shuffle Write Metrics") public ShuffleWriteMetrics shuffleWriteMetrics;
  @SerializedName("Input Metrics") public InputMetrics inputMetrics;
  @SerializedName("Output Metrics") public OutputMetrics outputMetrics;
  @SerializedName("Updated Blocks") public List<Block> updatedBlocks;
}
