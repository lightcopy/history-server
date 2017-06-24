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

import com.github.lightcopy.history.model.ApplicationLog;
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
    public List<ApplicationLog> applications() {
      List<ApplicationLog> list = new ArrayList<ApplicationLog>();
      list.add(
        new ApplicationLog(
          "Spark shell",
          "app-20170618085827-0000",
          1497733105297L,
          1497733151594L,
          "sadikovi",
          "file:/work/app-20170618085827-0000",
          26974L,
          1498271111876L,
          EventLog.Status.SUCCESS
        )
      );
      list.add(
        new ApplicationLog(
          "Spark shell",
          "app-20170616163546-0000",
          1497587745620L,
          1497587854143L,
          "sadikovi",
          "file:/work/app-20170616163546-0000",
          227041L,
          1498271111923L,
          EventLog.Status.SUCCESS
        )
      );
      list.add(
        new ApplicationLog(
          "Spark shell",
          "local-1497733035840",
          1497733033849L,
          1497733079367L,
          "sadikovi",
          "file:/work/local-1497733035840",
          26536L,
          1498271111959L,
          EventLog.Status.SUCCESS
        )
      );
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
