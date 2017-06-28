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

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.Environment;

/**
 * Interface for providing REST API calls implementation.
 */
public interface ApiProvider {

  /**
   * Return available applications.
   * Parameters are guaranteed to be valid.
   * @param page page number, 1-based
   * @param pageSize size of records per page, > 0
   * @param sortBy field name to sort by
   * @param asc return is ascending order if true, descending otherwise
   * @return list of Application instances.
   */
  List<Application> applications(int page, int pageSize, String sortBy, boolean asc);

  /**
   * Return application for provided appId.
   * Result can be null, if application is not found.
   * @param appId
   * @return Application instance or null, if not found
   *
   */
  Application application(String appId);

  /**
   * Return environment for provided appId.
   * Result can be null, if environment is not found.
   * @param appId
   * @return valid environment or null, if not found
   */
  Environment environment(String appId);
}
