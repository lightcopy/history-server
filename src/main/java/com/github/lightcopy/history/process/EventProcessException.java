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

package com.github.lightcopy.history.process;

/**
 * Event processing exception is thrown when parsing of individual event or aggregation fails,
 * or IO read fails, or any related to file processing operation. This does not include thread
 * management.
 */
public class EventProcessException extends Exception {
  public EventProcessException(String message, Throwable cause) {
    super(message, cause);
  }

  public EventProcessException(String message) {
    super(message);
  }
}
