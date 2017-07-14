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

package com.github.lightcopy.history;

/**
 * History server metadata.
 */
public class Metadata {
  // total number of available applications
  private long numApplications;

  public Metadata() {
    this.numApplications = 0;
  }

  /** Get number of applications */
  public long getNumApplications() {
    return numApplications;
  }

  /** Set number of applications */
  public void setNumApplications(long value) {
    this.numApplications = value;
  }

  @Override
  public String toString() {
    return "Metadata(numApplications=" + numApplications + ")";
  }
}
