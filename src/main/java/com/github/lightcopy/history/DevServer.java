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

import com.github.lightcopy.history.model.Application;

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
    public List<Application> applications(int page, int pageSize, String sortBy, boolean asc) {
      LOG.info("apps: page={}, pageSize={}, sortBy={}, asc={}", page, pageSize, sortBy, asc);
      List<Application> list = new ArrayList<Application>();
      Application app = new Application();
      app.setAppId("app-20170618085827-0000");
      app.setAppName("Spark shell");
      app.setStartTime(1497733105297L);
      app.setEndTime(1497733151594L);
      app.setUser("sadikovi");
      app.setInProgress(false);
      app.setPath("file:/work/app-20170618085827-0000");
      app.setSize(26974L);
      app.setModificationTime(1498271111876L);
      app.setStatus(Application.Status.SUCCESS);
      list.add(app);

      app = new Application();
      app.setAppId("app-20170616163546-0000");
      app.setAppName("Spark shell");
      app.setStartTime(1497587745620L);
      app.setEndTime(1497587854143L);
      app.setUser("sadikovi");
      app.setInProgress(false);
      app.setPath("file:/work/app-20170616163546-000");
      app.setSize(227041L);
      app.setModificationTime(1498271111923L);
      app.setStatus(Application.Status.SUCCESS);
      list.add(app);

      app = new Application();
      app.setAppId("local-1497733035840");
      app.setAppName("Spark shell");
      app.setStartTime(1497733033849L);
      app.setEndTime(1497733079367L);
      app.setUser("sadikovi");
      app.setInProgress(false);
      app.setPath("file:/work/local-1497733035840");
      app.setSize(26536L);
      app.setModificationTime(1498271111959L);
      app.setStatus(Application.Status.SUCCESS);
      list.add(app);

      return list;
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
