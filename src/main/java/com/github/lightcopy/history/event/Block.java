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

public class Block {
  public static class BlockStatus {
    @SerializedName("Storage Level") public StorageLevel storageLevel;
    @SerializedName("Memory Size") public long memorySize;
    @SerializedName("Disk Size") public long diskSize;
  }

  @SerializedName("Block ID") public String blockId;
  @SerializedName("Status") public BlockStatus status;
}
