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
import java.util.HashMap;
import java.util.List;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.ApplicationSummary;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Executor;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
import com.github.lightcopy.history.model.Task;

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
      app.setAppStatus(Application.AppStatus.FINISHED);
      app.setPath("file:/work/app-20170618085827-0000");
      app.setSize(26974L);
      app.setModificationTime(1498271111876L);
      app.setLoadStatus(Application.LoadStatus.LOAD_SUCCESS);
      list.add(app);

      app = new Application();
      app.setAppId("app-20170616163546-0000");
      app.setAppName("Spark shell");
      app.setStartTime(1497587745620L);
      app.setEndTime(1497587854143L);
      app.setUser("sadikovi");
      app.setAppStatus(Application.AppStatus.FINISHED);
      app.setPath("file:/work/app-20170616163546-000");
      app.setSize(227041L);
      app.setModificationTime(1498271111923L);
      app.setLoadStatus(Application.LoadStatus.LOAD_SUCCESS);
      list.add(app);

      app = new Application();
      app.setAppId("local-1497733035840");
      app.setAppName("Spark shell");
      app.setStartTime(1497733033849L);
      app.setEndTime(1497733079367L);
      app.setUser("sadikovi");
      app.setAppStatus(Application.AppStatus.FINISHED);
      app.setPath("file:/work/local-1497733035840");
      app.setSize(26536L);
      app.setModificationTime(1498271111959L);
      app.setLoadStatus(Application.LoadStatus.LOAD_SUCCESS);
      list.add(app);

      return list;
    }

    @Override
    public Application application(String appId) {
      Application app = new Application();
      app.setAppId(appId);
      app.setAppName("Sample application");
      app.setStartTime(1497733105297L);
      app.setEndTime(1497733151594L);
      app.setUser("sadikovi");
      app.setAppStatus(Application.AppStatus.FINISHED);
      app.setPath("file:/work/app-20170618085827-0000");
      app.setSize(26974L);
      app.setModificationTime(1498271111876L);
      app.setLoadStatus(Application.LoadStatus.LOAD_SUCCESS);
      return app;
    }

    @Override
    public ApplicationSummary appSummary(String appId) {
      ApplicationSummary sum = new ApplicationSummary();
      sum.setRunningQueries(3);
      sum.setCompletedQueries(12);
      return sum;
    }

    @Override
    public Environment environment(String appId) {
      Environment env = new Environment();

      HashMap<String, String> jvmProps = new HashMap<String, String>();
      jvmProps.put("Java Home", "/usr/local/share/jdk1.8.0_131/jre");
      jvmProps.put("Java Version", "1.8.0_131 (Oracle Corporation)");
      jvmProps.put("Scala Version", "version 2.11.8");
      env.setJvmInformation(jvmProps);

      HashMap<String, String> sparkProps = new HashMap<String, String>();
      sparkProps.put("spark.app.id", "app-20170628110009-0019");
      sparkProps.put("spark.driver.memory", "40g");
      sparkProps.put("spark.eventLog.enabled", "true");
      env.setSparkProperties(sparkProps);

      HashMap<String, String> sysProps = new HashMap<String, String>();
      sysProps.put("os.arch", "amd64");
      sysProps.put("os.name", "Linux");
      sysProps.put("os.version", "4.4.0-79-generic");
      env.setSystemProperties(sysProps);

      HashMap<String, String> clsProps = new HashMap<String, String>();
      clsProps.put("/usr/local/spark/jars/spark-catalyst_2.11-2.1.0.jar", "System Classpath");
      clsProps.put("/usr/local/spark/jars/spark-core_2.11-2.1.0.jar", "System Classpath");
      clsProps.put("/usr/local/spark/jars/spark-graphx_2.11-2.1.0.jar", "System Classpath");
      env.setClasspathEntries(clsProps);

      return env;
    }

    @Override
    public List<SQLExecution> sqlExecutions(
        String appId, int page, int pageSize, String sortBy, boolean asc) {
      List<SQLExecution> list = new ArrayList<SQLExecution>();
      for (int i = 0; i < 5; i++) {
        list.add(sqlExecution(appId, i));
      }
      return list;
    }

    @Override
    public SQLExecution sqlExecution(String appId, int executionId) {
      SQLExecution sql = new SQLExecution();
      sql.setAppId(appId);
      sql.setExecutionId(executionId);
      sql.setDescription("count at <console>:24");
      sql.setDetails("org.apache.spark.sql.Dataset.count(Dataset.scala:2419)");
      sql.setPhysicalPlan("== Parsed Logical Plan ==");
      sql.setStartTime(1498724267295L);
      sql.setEndTime(1498724277381L);
      sql.updateDuration();
      sql.setStatus(SQLExecution.Status.COMPLETED);
      sql.addJobId(0);
      sql.addJobId(1);
      sql.addJobId(2);
      return sql;
    }

    private Executor generateExecutor(String appId, String executorId, Executor.Status status) {
      Executor exc = new Executor();
      exc.setAppId(appId);
      exc.setExecutorId(executorId);
      exc.updateSortExecutorId();
      exc.setHost("10.583.8.16");
      exc.setPort(45323);
      exc.setCores(16);
      exc.setMaxMemory(512 * 1000L);
      if (status == Executor.Status.ACTIVE) {
        exc.setStartTime(System.currentTimeMillis());
        exc.setEndTime(-1L);
        exc.setFailureReason(null);
      } else {
        exc.setStartTime(System.currentTimeMillis() - 19000000L);
        exc.setEndTime(System.currentTimeMillis());
        exc.setFailureReason("Failure");
      }
      exc.updateDuration();
      exc.setStatus(status);
      HashMap<String, String> logs = new HashMap<String, String>();
      logs.put("stdout", "http://stdout");
      logs.put("stderr", "http://stdout");
      exc.setLogs(logs);
      exc.setActiveTasks(1000);
      exc.setCompletedTasks(2000);
      exc.setFailedTasks(3000);
      exc.setTotalTasks(6000);
      exc.setTaskTime(100000L);
      return exc;
    }

    @Override
    public List<Executor> executors(
        String appId, Executor.Status status, int page, int pageSize, String sortBy, boolean asc) {
      ArrayList<Executor> list = new ArrayList<Executor>();
      list.add(generateExecutor(appId, "driver", Executor.Status.ACTIVE));
      list.add(generateExecutor(appId, "0", Executor.Status.ACTIVE));
      list.add(generateExecutor(appId, "1", Executor.Status.ACTIVE));
      list.add(generateExecutor(appId, "2", Executor.Status.ACTIVE));
      list.add(generateExecutor(appId, "10", Executor.Status.ACTIVE));

      list.add(generateExecutor(appId, "3", Executor.Status.REMOVED));
      list.add(generateExecutor(appId, "4", Executor.Status.REMOVED));
      list.add(generateExecutor(appId, "5", Executor.Status.REMOVED));

      return list;
    }

    @Override
    public List<Stage> stages(String appId, int page, int pageSize, String sortBy, boolean asc) {
      return new ArrayList<Stage>();
    }

    @Override
    public List<Stage> stages(
        String appId, int jobId, int page, int pageSize, String sortBy, boolean asc) {
      return new ArrayList<Stage>();
    }

    @Override
    public Stage stage(String appId, int stageId, int stageAttemptId) {
      return null;
    }

    @Override
    public List<Task> tasks(String appId, int stageId, int stageAttemptId, int page, int pageSize,
        String sortBy, boolean asc) {
      return new ArrayList<Task>();
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
