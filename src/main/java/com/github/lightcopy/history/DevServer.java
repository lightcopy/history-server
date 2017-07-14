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
import java.util.HashSet;
import java.util.List;

import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.ApplicationSummary;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Executor;
import com.github.lightcopy.history.model.Job;
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
    public Metadata metadata() {
      Metadata meta = new Metadata();
      meta.setNumApplications(3);
      return meta;
    }

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

      HashSet<String> activeExecutors = new HashSet<String>();
      activeExecutors.add("driver");
      activeExecutors.add("0");
      activeExecutors.add("1");
      activeExecutors.add("2");
      activeExecutors.add("10");
      sum.setActiveExecutors(activeExecutors);

      HashSet<String> removedExecutors = new HashSet<String>();
      removedExecutors.add("3");
      removedExecutors.add("4");
      removedExecutors.add("5");
      sum.setRemovedExecutors(removedExecutors);

      HashMap<String, ApplicationSummary.JobSummary> jobs =
        new HashMap<String, ApplicationSummary.JobSummary>();
      ApplicationSummary.JobSummary jobSummary = new ApplicationSummary.JobSummary();
      jobSummary.pendingStages = 1;
      jobSummary.activeStages = 2;
      jobSummary.completedStages = 3;
      jobSummary.failedStages = 4;
      jobSummary.skippedStages = 5;
      jobs.put("1", jobSummary);
      sum.setRunningJobs(jobs);
      sum.setSucceededJobs(jobs);
      sum.setFailedJobs(jobs);

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
      if (status == Executor.Status.ACTIVE) {
        list.add(generateExecutor(appId, "0", Executor.Status.ACTIVE));
        list.add(generateExecutor(appId, "1", Executor.Status.ACTIVE));
        list.add(generateExecutor(appId, "2", Executor.Status.ACTIVE));
        list.add(generateExecutor(appId, "10", Executor.Status.ACTIVE));
        list.add(generateExecutor(appId, "driver", Executor.Status.ACTIVE));
      } else if (status == Executor.Status.REMOVED) {
        list.add(generateExecutor(appId, "3", Executor.Status.REMOVED));
        list.add(generateExecutor(appId, "4", Executor.Status.REMOVED));
        list.add(generateExecutor(appId, "5", Executor.Status.REMOVED));
      }
      return list;
    }

    private Job generateJob(String appId, int jobId, String name, Job.Status status) {
      Job job = new Job();
      job.setAppId(appId);
      job.setJobId(jobId);
      job.setJobName(name);
      job.setStartTime(System.currentTimeMillis() - 1990000L);
      job.setEndTime(System.currentTimeMillis() - 10000L);
      job.updateDuration();
      job.setStatus(status);
      job.setErrorDescription("Error");
      job.setErrorDetails("Error details");
      job.setActiveTasks(10);
      job.setCompletedTasks(20);
      job.setFailedTasks(10);
      job.setSkippedTasks(10);
      job.setTotalTasks(50);
      job.markStagePending(0, 0);
      job.markStageActive(1, 0);
      job.markStageCompleted(2, 0);
      job.markStageFailed(3, 0);
      job.markStageSkipped(4, 0);
      return job;
    }

    @Override
    public List<Job> jobs(
        String appId, Job.Status status, int page, int pageSize, String sortBy, boolean asc) {
      ArrayList<Job> list = new ArrayList<Job>();
      if (status == Job.Status.RUNNING) {
        list.add(generateJob(appId, 4, "count at <console>:26", status));
      } else if (status == Job.Status.SUCCEEDED) {
        list.add(generateJob(appId, 3, "foreach at <console>:26", status));
        list.add(generateJob(appId, 2, "foreach at <console>:26", status));
      } else if (status == Job.Status.FAILED) {
        list.add(generateJob(appId, 1, "first at <console>:26", status));
      }
      return list;
    }

    @Override
    public Job job(String appId, int jobId) {
      return generateJob(appId, 2, "foreach at <console>:26", Job.Status.SUCCEEDED);
    }

    private Stage generateStage(
        String appId, int stageId, int attempt, String name, Stage.Status status) {
      Stage stage = new Stage();
      stage.setAppId(appId);
      stage.setJobId(1);
      stage.setUniqueStageId(((long) stageId) << 32 | attempt);
      stage.setStageId(stageId);
      stage.setStageAttemptId(attempt);
      stage.setStageName(name);
      stage.setActiveTasks(10);
      stage.setCompletedTasks(12);
      stage.setFailedTasks(4);
      stage.setTotalTasks(26);
      stage.setDetails("Stage details");
      stage.setStartTime(System.currentTimeMillis() - 19000000L);
      stage.setEndTime(System.currentTimeMillis() - 100000L);
      stage.setDuration(19000000L);
      stage.setStatus(status);
      stage.setErrorDescription("Error reason");
      stage.setErrorDetails("Error details");
      return stage;
    }

    @Override
    public List<Stage> stages(
        String appId, Stage.Status status, int page, int pageSize, String sortBy, boolean asc) {
      ArrayList<Stage> list = new ArrayList<Stage>();
      if (status == Stage.Status.PENDING) {
        list.add(generateStage(appId, 1, 0, "foreach at <console>:26", status));
        list.add(generateStage(appId, 0, 0, "count at <console>:26", status));
      } else if (status == Stage.Status.SKIPPED) {
        list.add(generateStage(appId, 2, 0, "collect at <console>:26", status));
      } else if (status == Stage.Status.ACTIVE) {
        list.add(generateStage(appId, 4, 0, "collect at <console>:26", status));
      } else if (status == Stage.Status.COMPLETED) {
        list.add(generateStage(appId, 3, 2, "first at <console>:26", status));
      } else if (status == Stage.Status.FAILED) {
        list.add(generateStage(appId, 3, 1, "show at <console>:26", status));
        list.add(generateStage(appId, 3, 0, "count at <console>:26", status));
      }
      return list;
    }

    @Override
    public List<Stage> stagesForJob(String appId, int jobId,
        Stage.Status status, int page, int pageSize, String sortBy, boolean asc) {
      return stages(appId, status, page, pageSize, sortBy, asc);
    }

    @Override
    public Stage stage(String appId, int stageId, int stageAttemptId) {
      return generateStage(appId, stageId, stageAttemptId, "show at <console>:26",
        Stage.Status.COMPLETED);
    }

    @Override
    public List<Task> tasks(String appId, int stageId, int stageAttemptId, int page, int pageSize,
        String sortBy, boolean asc) {
      ArrayList<Task> list = new ArrayList<Task>();
      Task task = new Task();
      task.setTaskId(100000L);
      task.setStageId(stageId);
      task.setStageAttemptId(stageAttemptId);
      task.setIndex(123);
      task.setAttempt(1);
      task.setStartTime(System.currentTimeMillis() - 19000000L);
      task.setEndTime(System.currentTimeMillis());
      task.setDuration(19000000L);
      task.setExecutorId("1");
      task.setHost("host");
      task.setLocality("NODE_LOCAL");
      task.setStatus(Task.Status.SUCCESS);
      task.setErrorDescription("Error");
      task.setErrorDetails("Details");
      list.add(task);
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
