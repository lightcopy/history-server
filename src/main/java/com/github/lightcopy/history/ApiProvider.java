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
import com.github.lightcopy.history.model.ApplicationSummary;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.Executor;
import com.github.lightcopy.history.model.Job;
import com.github.lightcopy.history.model.SQLExecution;
import com.github.lightcopy.history.model.Stage;
import com.github.lightcopy.history.model.StageSummary;
import com.github.lightcopy.history.model.Task;

/**
 * Interface for providing REST API calls implementation.
 */
public interface ApiProvider {

  /**
   * Return metadata of history server.
   * @return metadata
   */
  Metadata metadata();

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
   * Return application summary for provided appId.
   * Result can be null, if application summary is not found.
   * @param appId
   * @return application summary
   */
  ApplicationSummary appSummary(String appId);

  /**
   * Return environment for provided appId.
   * Result can be null, if environment is not found.
   * @param appId
   * @return valid environment or null, if not found
   */
  Environment environment(String appId);

  /**
   * Return available SQL executions.
   * Parameters are guaranteed to be valid.
   * @param appId application id
   * @param page page number, 1-based
   * @param pageSize size of records per page, > 0
   * @param sortBy field name to sort by
   * @param asc return is ascending order if true, descending otherwise
   * @return list of SQLExecution instances.
   */
  List<SQLExecution> sqlExecutions(
      String appId, int page, int pageSize, String sortBy, boolean asc);

  /**
   * Return SQL execution for provided appId and executionId.
   * Result can be null, if execution is not found.
   * @param appId
   * @param executionId
   * @return valid sql execution or null, if not found
   */
  SQLExecution sqlExecution(String appId, int executionId);

  /**
   * Return list of executors based on provided appId and status.
   * @param appId application id
   * @param status executor status
   * @param page page number
   * @param pageSize page size
   * @param sortBy field name to sort by
   * @param asc ascending sort if true, descending otherwise
   * @return list of executors
   */
  List<Executor> executors(
      String appId, Executor.Status status, int page, int pageSize, String sortBy, boolean asc);

  /**
   * Return list of jobs based on provided appId and job status.
   * @param appId application id
   * @param status job status
   * @param page page number
   * @param pageSize page size
   * @param sortBy field name to sort by
   * @param asc ascending sort if true, descending otherwise
   * @return list of jobs
   */
  List<Job> jobs(
      String appId, Job.Status status, int page, int pageSize, String sortBy, boolean asc);

  /**
   * Return job for provided appId and jobId.
   * Result can be null, if job is not found.
   * @param appId
   * @param jobId
   * @return valid job or null, if not found
   */
  Job job(String appId, int jobId);

  /**
   * Return list of stages for appid.
   * @param appId application id
   * @param status stage status
   * @param page page number, 1-based
   * @param pageSize size of records per page, > 0
   * @param sortBy field name to sort by
   * @param asc return is ascending order if true, descending otherwise
   * @return list of Stage instances.
   */
  List<Stage> stages(
      String appId, Stage.Status status, int page, int pageSize, String sortBy, boolean asc);

  /**
   * Return list of stages for appid and jobId.
   * @param appId application id
   * @param jobId job id
   * @param status stage status
   * @param page page number, 1-based
   * @param pageSize size of records per page, > 0
   * @param sortBy field name to sort by
   * @param asc return is ascending order if true, descending otherwise
   * @return list of Stage instances.
   */
  List<Stage> stagesForJob(String appId, int jobId,
      Stage.Status status, int page, int pageSize, String sortBy, boolean asc);

  /**
   * Return Stage instance for appId, stageId and attempt number.
   * Result can be null, if stage is not found.
   * @param appId application id
   * @param stageId
   * @param stageAttemptId
   * @return valid stage or null, if not found
   */
  Stage stage(String appId, int stageId, int stageAttemptId);

  /**
   * Return StageSummary instance for appId, stageId and attempt number.
   * Result can be null, if stage summary is not found.
   * @param appId application id
   * @param stageId
   * @param stageAttemptId
   * @return valid stage summary or null, if not found
   */
  StageSummary stageSummary(String appId, int stageId, int stageAttemptId);

  /**
   * Return list of tasks for a particular stage attempt.
   * @param appId application id
   * @param stageId stage id
   * @param stageAttemptId attempt number
   * @param page page number, 1-based
   * @param pageSize size of records per page, > 0
   * @param sortBy field name to sort by
   * @param asc return is ascending order if true, descending otherwise
   * @return list of Task instances.
   */
  List<Task> tasks(String appId, int stageId, int stageAttemptId, int page, int pageSize,
      String sortBy, boolean asc);
}
