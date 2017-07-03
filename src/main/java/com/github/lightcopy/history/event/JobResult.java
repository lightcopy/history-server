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

public class JobResult {
  // decomposed exception
  public static class ResultException {
    @SerializedName("Message") public String message;
    // currently we do not use stack trace
    @SerializedName("Stack Trace") public Object stackTrace;
  }

  @SerializedName("Result") public String result;
  @SerializedName("Exception") public ResultException exception;

  public boolean isSuccess() {
    return result != null && result.equals("JobSucceeded");
  }

  public String getDescription() {
    // return first line as description, if available
    if (exception != null && exception.message != null) {
      int maxChars = 200;
      int index = exception.message.indexOf('\n');
      if (index > maxChars) {
        return exception.message.substring(0, maxChars).trim() + "...";
      } else {
        return exception.message.substring(0, index).trim();
      }
    }
    return "";
  }

  public String getDetails() {
    return (exception != null) ? exception.message : null;
  }
}
