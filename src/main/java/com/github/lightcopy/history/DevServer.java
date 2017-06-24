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

import java.util.ArrayList;
import java.util.List;

import com.github.lightcopy.history.model.EventLog;

/** Simple development server for frontend */
public class DevServer extends AbstractServer {

  public DevServer() {
    super();
  }

  @Override
  public ApiProvider apiProvider() {
    return new TestApiProvider();
  }

  /** Test API provider with hardcoded examples of data */
  static class TestApiProvider implements ApiProvider {
    @Override
    public List<EventLog> eventLogs() {
      return new ArrayList<EventLog>();
    }
  }

  public static void main(String[] args) {
    try {
      LOG.info("Initialize dev web server");
      DevServer server = new DevServer();
      LOG.info("Created server {}", server);
      server.launch();
    } catch (Exception err) {
      LOG.error("Exception occurred", err);
      System.exit(1);
    }
  }
}
