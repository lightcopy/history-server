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

import java.util.List;

import com.github.lightcopy.history.model.ApplicationLog;

/**
 * Interface for providing REST API calls implementation.
 */
public interface ApiProvider {

  /**
   * Return available application logs.
   * Parameters are guaranteed to be valid.
   * @param page page number, 1-based
   * @param pageSize size of records per page, > 0
   * @param sortBy field name to sort by
   * @param asc return is ascending order if true, descending otherwise
   * @return list of ApplicationLog instances.
   */
  List<ApplicationLog> applications(int page, int pageSize, String sortBy, boolean asc);
}