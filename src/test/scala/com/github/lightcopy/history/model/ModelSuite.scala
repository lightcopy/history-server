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

package com.github.lightcopy.history.model

import java.util.HashMap

import org.apache.hadoop.fs.Path
import org.bson.{BsonDocument, BsonDocumentWriter, BsonDocumentReader}
import org.bson.codecs.{DecoderContext, EncoderContext}

import com.github.lightcopy.history.event.StageInfo
import com.github.lightcopy.history.event.TaskEndReason
import com.github.lightcopy.history.event.TaskInfo
import com.github.lightcopy.history.event.TaskMetrics
import com.github.lightcopy.testutil.UnitTestSuite

class ModelSuite extends UnitTestSuite {

  def serialize[T](codec: AbstractCodec[T], obj: T): BsonDocument = {
    val doc = new BsonDocument()
    val writer = new BsonDocumentWriter(doc)
    val encoder = EncoderContext.builder().build()
    codec.encode(writer, obj, encoder)
    doc
  }

  def deserialize[T](codec: AbstractCodec[T], doc: BsonDocument): T = {
    val reader = new BsonDocumentReader(doc)
    val decoder = DecoderContext.builder().build()
    codec.decode(reader, decoder)
  }

  def hm(map: Map[String, String]): java.util.HashMap[String, String] = {
    val res = new java.util.HashMap[String, String]()
    for ((key, value) <- map) {
      res.put(key, value)
    }
    res
  }

  def al[T](seq: Seq[T]): java.util.ArrayList[T] = {
    val res = new java.util.ArrayList[T]()
    for (item <- seq) {
      res.add(item)
    }
    res
  }

  test("Create application") {
    val app = new Application()
    app.setAppId("app-id")
    app.setAppName("app-name")
    app.setStartTime(1000L)
    app.setEndTime(2000L)
    app.setUser("user")
    app.setAppStatus(Application.AppStatus.FINISHED)
    app.setPath("file:/tmp")
    app.setSize(128L)
    app.setModificationTime(3000L)
    app.setLoadStatus(Application.LoadStatus.LOAD_SUCCESS)

    app.getAppName() should be ("app-name")
    app.getAppId() should be ("app-id")
    app.getStartTime() should be (1000L)
    app.getEndTime() should be (2000L)
    app.getUser() should be ("user")
    app.getAppStatus() should be (Application.AppStatus.FINISHED)
    app.getPath() should be ("file:/tmp")
    app.getSize() should be (128L)
    app.getModificationTime() should be (3000L)
    app.getLoadStatus() should be (Application.LoadStatus.LOAD_SUCCESS)
  }

  test("Create empty application") {
    val app = new Application()

    app.getAppId() should be (null)
    app.getAppName() should be (null)
    app.getStartTime() should be (-1L)
    app.getEndTime() should be (-1L)
    app.getUser() should be (null)
    app.getAppStatus() should be (Application.AppStatus.NONE)
    app.getPath() should be (null)
    app.getSize() should be (0L)
    app.getModificationTime() should be (-1L)
    app.getLoadStatus() should be (Application.LoadStatus.LOAD_PROGRESS)
  }

  test("Empty application to bson") {
    val app = new Application()
    val doc = serialize(app, app)
    val res = deserialize(app, doc)

    res.getAppId() should be (app.getAppId())
    res.getAppName() should be (app.getAppName())
    res.getStartTime() should be (app.getStartTime())
    res.getEndTime() should be (app.getEndTime())
    res.getUser() should be (app.getUser())
    res.getAppStatus() should be (app.getAppStatus())
    res.getPath() should be (app.getPath())
    res.getSize() should be (app.getSize())
    res.getModificationTime() should be (app.getModificationTime())
    res.getLoadStatus() should be (app.getLoadStatus())
  }

  test("Complete application to bson") {
    val app = new Application()
    app.setAppId("app-id")
    app.setAppName("app-name")
    app.setStartTime(1000L)
    app.setEndTime(2000L)
    app.setUser("user")
    app.setAppStatus(Application.AppStatus.FINISHED)
    app.setPath("file:/tmp")
    app.setSize(128L)
    app.setModificationTime(3000L)
    app.setLoadStatus(Application.LoadStatus.LOAD_SUCCESS)

    val doc = serialize(app, app)
    val res = deserialize(app, doc)

    res.getAppId() should be ("app-id")
    res.getAppName() should be ("app-name")
    res.getStartTime() should be (1000L)
    res.getEndTime() should be (2000L)
    res.getUser() should be ("user")
    app.getAppStatus() should be (Application.AppStatus.FINISHED)
    app.getPath() should be ("file:/tmp")
    app.getSize() should be (128L)
    app.getModificationTime() should be (3000L)
    app.getLoadStatus() should be (Application.LoadStatus.LOAD_SUCCESS)
  }

  test("Empty environment to bson") {
    val env = new Environment()

    val doc = serialize(env, env)
    val res = deserialize(env, doc)

    res.getAppId() should be (env.getAppId())
    res.getJvmInformation() should be (env.getJvmInformation())
    res.getSparkProperties() should be (env.getSparkProperties())
    res.getSystemProperties() should be (env.getSystemProperties())
    res.getClasspathEntries() should be (env.getClasspathEntries())
  }

  test("Complete environment into bson") {
    import Environment._
    val env = new Environment()
    env.setAppId("app-id")
    env.setJvmInformation(hm(Map("a.1" -> "b.1", "a.2" -> "b.2")))
    env.setSparkProperties(hm(Map("c.1" -> "d.1", "c.2" -> "d.2")))
    env.setSystemProperties(hm(Map("e.1" -> "f.1", "e.2" -> "f.2")))
    env.setClasspathEntries(hm(Map("g.1" -> "h.1", "g.2" -> "h.2")))

    val doc = serialize(env, env)
    val res = deserialize(env, doc)

    res.getAppId() should be ("app-id")
    res.getJvmInformation() should be (
      al(new Entry("a.1", "b.1") :: new Entry("a.2", "b.2") :: Nil))
    res.getSparkProperties() should be (
      al(new Entry("c.1", "d.1") :: new Entry("c.2", "d.2") :: Nil))
    res.getSystemProperties() should be (
      al(new Entry("e.1", "f.1") :: new Entry("e.2", "f.2") :: Nil))
    res.getClasspathEntries() should be (
      al(new Entry("g.1", "h.1") :: new Entry("g.2", "h.2") :: Nil))
  }

  test("Sort entries for environment") {
    import Environment._
    val env = new Environment()
    val map = Map("a" -> "1", "e" -> "2", "b" -> "3", "d" -> "5", "c" -> "4")
    env.setAppId("app-id")
    env.setJvmInformation(hm(map))
    env.setSparkProperties(hm(map))
    env.setSystemProperties(hm(map))
    env.setClasspathEntries(hm(map))

    val lst = al(Seq(
      new Entry("a", "1"),
      new Entry("b", "3"),
      new Entry("c", "4"),
      new Entry("d", "5"),
      new Entry("e", "2")
    ))

    env.getJvmInformation() should be (lst)
    env.getSparkProperties() should be (lst)
    env.getSystemProperties() should be (lst)
    env.getClasspathEntries() should be (lst)

    val doc = serialize(env, env)
    val res = deserialize(env, doc)

    res.getJvmInformation() should be (lst)
    res.getSparkProperties() should be (lst)
    res.getSystemProperties() should be (lst)
    res.getClasspathEntries() should be (lst)
  }

  test("Empty SQLExecution to bson") {
    val sql = new SQLExecution()

    val doc = serialize(sql, sql)
    val res = deserialize(sql, doc)

    res.getAppId should be (sql.getAppId)
    res.getExecutionId should be (sql.getExecutionId)
    res.getDescription should be (sql.getDescription)
    res.getDetails should be (sql.getDetails)
    res.getPhysicalPlan should be (sql.getPhysicalPlan)
    res.getStartTime should be (sql.getStartTime)
    res.getEndTime should be (sql.getEndTime)
    res.getDuration should be (sql.getDuration)
    res.getStatus should be (sql.getStatus)
    res.getJobIds should be (sql.getJobIds)
  }

  test("Complete SQLExecution to bson") {
    val sql = new SQLExecution()
    sql.setAppId("app-123")
    sql.setExecutionId(3)
    sql.setDescription("count at <console>:24")
    sql.setDetails("org.apache.spark.sql.Dataset.count(Dataset.scala:2419)")
    sql.setPhysicalPlan("== Parsed Logical Plan ==")
    sql.setStartTime(1498724267295L)
    sql.setEndTime(1498724277381L)
    sql.updateDuration()
    sql.setStatus(SQLExecution.Status.COMPLETED)
    sql.addJobId(1)
    sql.addJobId(2)

    val doc = serialize(sql, sql)
    val res = deserialize(sql, doc)

    res.getAppId should be ("app-123")
    res.getExecutionId should be (3)
    res.getDescription should be ("count at <console>:24")
    res.getDetails should be ("org.apache.spark.sql.Dataset.count(Dataset.scala:2419)")
    res.getPhysicalPlan should be ("== Parsed Logical Plan ==")
    res.getStartTime should be (1498724267295L)
    res.getEndTime should be (1498724277381L)
    res.getDuration should be (1498724277381L - 1498724267295L)
    res.getStatus should be (SQLExecution.Status.COMPLETED)
    res.getJobIds should be (al(Seq(1, 2)))
  }

  test("Empty task to bson") {
    val task = new Task()
    val doc = serialize(task, task)
    val res = deserialize(task, doc)

    res.getTaskId should be (task.getTaskId)
    res.getStageId should be (task.getStageId)
    res.getStageAttemptId should be (task.getStageAttemptId)
    res.getIndex should be (task.getIndex)
    res.getAttempt should be (task.getAttempt)
    res.getStartTime should be (task.getStartTime)
    res.getEndTime should be (task.getEndTime)
    res.getExecutorId should be (task.getExecutorId)
    res.getHost should be (task.getHost)
    res.getLocality should be (task.getLocality)
    res.getSpeculative should be (task.getSpeculative)
    res.getStatus should be (task.getStatus)
    res.getDuration should be (task.getDuration)
    res.getErrorDescription should be (task.getErrorDescription)
    res.getErrorDetails should be (task.getErrorDetails)
    res.getMetrics() should be (task.getMetrics())
  }

  test("Complete task to bson") {
    val task = new Task()
    task.setTaskId(100000L)
    task.setStageId(2)
    task.setStageAttemptId(1)
    task.setIndex(123)
    task.setAttempt(1)
    task.setStartTime(7777777L)
    task.setEndTime(99999999L)
    task.setExecutorId("123")
    task.setHost("host")
    task.setLocality("NODE_LOCAL")
    task.setSpeculative(true)
    task.setStatus(Task.Status.SUCCESS)
    task.setDuration(2222222L)
    task.setErrorDescription("Error")
    task.setErrorDetails("Details")
    val metrics = new Metrics()
    metrics.setResultSize(123L)
    task.setMetrics(metrics)

    val doc = serialize(task, task)
    val res = deserialize(task, doc)

    res.getTaskId should be (100000L)
    res.getStageId should be (2)
    res.getStageAttemptId should be (1)
    res.getIndex should be (123)
    res.getAttempt should be (1)
    res.getStartTime should be (7777777L)
    res.getEndTime should be (99999999L)
    res.getExecutorId should be ("123")
    res.getHost should be ("host")
    res.getLocality should be ("NODE_LOCAL")
    res.getSpeculative should be (true)
    res.getStatus should be (Task.Status.SUCCESS)
    res.getDuration should be (2222222L)
    res.getErrorDescription should be ("Error")
    res.getErrorDetails should be ("Details")
    res.getMetrics should be (metrics)
  }

  test("Task from empty TaskInfo") {
    val task = new Task()
    task.update(new TaskInfo())
    val doc = serialize(task, task)
    val res = deserialize(task, doc)

    res.getTaskId should be (0L)
    res.getStageId should be (-1L)
    res.getStageAttemptId should be (-1L)
    res.getIndex should be (0)
    res.getAttempt should be (0)
    res.getStartTime should be (-1L)
    res.getEndTime should be (-1L)
    res.getExecutorId should be (null)
    res.getHost should be (null)
    res.getLocality should be (null)
    res.getSpeculative should be (false)
    res.getStatus should be (Task.Status.RUNNING)
    res.getDuration should be (-1L)
  }

  test("Task from TaskInfo") {
    val info = new TaskInfo()
    info.taskId = 123L
    info.index = 1
    info.attempt = 2
    info.launchTime = 188L
    info.executorId = "102"
    info.host = "host"
    info.locality = "NODE_LOCAL"
    info.speculative = true
    info.gettingResultTime = 0L
    info.finishTime = 190L
    info.failed = false
    info.killed = false
    val task = new Task()
    task.setStageId(10)
    task.setStageAttemptId(1)

    task.update(info)
    val doc = serialize(task, task)
    val res = deserialize(task, doc)

    res.getTaskId should be (123L)
    res.getStageId should be (10)
    res.getStageAttemptId should be (1)
    res.getIndex should be (1)
    res.getAttempt should be (2)
    res.getStartTime should be (188L)
    res.getEndTime should be (190L)
    res.getExecutorId should be ("102")
    res.getHost should be ("host")
    res.getLocality should be ("NODE_LOCAL")
    res.getSpeculative should be (true)
    res.getStatus should be (Task.Status.SUCCESS)
    res.getDuration should be (2L)
  }

  test("Task update for TaskEndReason") {
    val reason = new TaskEndReason()
    val task = new Task()

    reason.reason = "Success"
    task.update(reason)
    task.getErrorDescription should be ("")
    task.getErrorDetails should be (null)

    reason.reason = "TaskKilled"
    reason.killReason = "Test"
    task.update(reason)
    task.getErrorDescription should be ("TaskKilled (Test)")
    task.getErrorDetails should be (null)
  }

  test("Task status GET_RESULT") {
    val info = new TaskInfo()
    info.launchTime = 100L
    info.gettingResultTime = 12L
    val task = new Task()

    task.update(info)
    task.getStatus should be (Task.Status.GET_RESULT)
  }

  test("Task status FAILED") {
    val info = new TaskInfo()
    info.launchTime = 100L
    info.finishTime = 200L
    info.failed = true
    val task = new Task()

    task.update(info)
    task.getStatus should be (Task.Status.FAILED)
  }

  test("Task status KILLED") {
    val info = new TaskInfo()
    info.launchTime = 100L
    info.finishTime = 200L
    info.killed = true
    val task = new Task()

    task.update(info)
    task.getStatus should be (Task.Status.KILLED)
  }

  test("Empty Metrics to bson") {
    val metrics = new Metrics()
    val doc = serialize(metrics, metrics)
    val res = deserialize(metrics, doc)
    res should be (metrics)
  }

  test("Complete metrics to bson") {
    import com.github.lightcopy.history.model.Metrics._

    val taskMetrics = new TaskMetrics()
    taskMetrics.executorDeserializeTime = 1L
    taskMetrics.executorDeserializeCpuTime = 2L
    taskMetrics.executorRunTime = 3L
    taskMetrics.executorCpuTime = 4L
    taskMetrics.resultSize = 5L
    taskMetrics.jvmGcTime = 6L
    taskMetrics.resultSerializationTime = 7L
    taskMetrics.memoryBytesSpilled = 10L
    taskMetrics.diskBytesSpilled = 11L

    taskMetrics.shuffleReadMetrics = new TaskMetrics.ShuffleReadMetrics()
    taskMetrics.shuffleReadMetrics.remoteBlocksFetched = 12L
    taskMetrics.shuffleReadMetrics.localBlocksFetched = 13L
    taskMetrics.shuffleReadMetrics.fetchWaitTime = 14L
    taskMetrics.shuffleReadMetrics.remoteBytesRead = 15L
    taskMetrics.shuffleReadMetrics.localBytesRead = 16L
    taskMetrics.shuffleReadMetrics.totalRecordsRead = 17L

    taskMetrics.shuffleWriteMetrics = new TaskMetrics.ShuffleWriteMetrics()
    taskMetrics.shuffleWriteMetrics.shuffleBytesWritten = 18L
    taskMetrics.shuffleWriteMetrics.shuffleWriteTime = 19L
    taskMetrics.shuffleWriteMetrics.shuffleRecordsWritten = 20L

    taskMetrics.inputMetrics = new TaskMetrics.InputMetrics()
    taskMetrics.inputMetrics.bytesRead = 21L
    taskMetrics.inputMetrics.recordsRead = 22L

    taskMetrics.outputMetrics = new TaskMetrics.OutputMetrics()
    taskMetrics.outputMetrics.bytesWritten = 23L
    taskMetrics.outputMetrics.recordsWritten = 24L

    val metrics = new Metrics()
    metrics.set(taskMetrics)
    val doc = serialize(metrics, metrics)
    val res = deserialize(metrics, doc)

    res.getResultSize should be (5L)
    res.getJvmGcTime should be (6L)
    res.getResultSerializationTime should be (7L)
    res.getMemoryBytesSpilled should be (10L)
    res.getDiskBytesSpilled should be (11L)

    res.getExecutorMetrics.get(EXECUTOR_DESERIALIZE_TIME) should be (1L)
    res.getExecutorMetrics.get(EXECUTOR_DESERIALIZE_CPU_TIME) should be (2L)
    res.getExecutorMetrics.get(EXECUTOR_RUN_TIME) should be (3L)
    res.getExecutorMetrics.get(EXECUTOR_CPU_TIME) should be (4L)
    res.getShuffleReadMetrics.get(SHUFFLE_REMOTE_BLOCKS_FETCHED) should be (12L)
    res.getShuffleReadMetrics.get(SHUFFLE_LOCAL_BLOCKS_FETCHED) should be (13L)
    res.getShuffleReadMetrics.get(SHUFFLE_FETCH_WAIT_TIME) should be (14L)
    res.getShuffleReadMetrics.get(SHUFFLE_REMOTE_BYTES_READ) should be (15L)
    res.getShuffleReadMetrics.get(SHUFFLE_LOCAL_BYTES_READ) should be (16L)
    res.getShuffleReadMetrics.get(SHUFFLE_TOTAL_RECORDS_READ) should be (17L)
    res.getShuffleWriteMetrics.get(SHUFFLE_BYTES_WRITTEN) should be (18L)
    res.getShuffleWriteMetrics.get(SHUFFLE_WRITE_TIME) should be (19L)
    res.getShuffleWriteMetrics.get(SHUFFLE_RECORDS_WRITTEN) should be (20L)
    res.getInputMetrics.get(INPUT_BYTES_READ) should be (21L)
    res.getInputMetrics.get(INPUT_RECORDS_READ) should be (22L)
    res.getOutputMetrics.get(OUTPUT_BYTES_WRITTEN) should be (23L)
    res.getOutputMetrics.get(OUTPUT_RECORDS_WRITTEN) should be (24L)
  }

  test("Merge empty metrics") {
    val metrics = new Metrics()
    // merge empty metrics, this should not change any values
    metrics.merge(new Metrics())
    metrics.merge(new Metrics())
    metrics should be (new Metrics())
  }

  test("Merge empty metrics with non-empty update") {
    import com.github.lightcopy.history.model.Metrics._

    val metrics = new Metrics()
    val update = new Metrics()

    update.setResultSize(100L)
    update.setJvmGcTime(200L)
    update.setResultSerializationTime(300L)
    update.setMemoryBytesSpilled(400L)
    update.setDiskBytesSpilled(500L)

    update.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_TIME, 600L)
    update.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, 700L)
    update.getExecutorMetrics.put(EXECUTOR_RUN_TIME, 800L)
    update.getExecutorMetrics.put(EXECUTOR_CPU_TIME, 900L)

    update.getShuffleReadMetrics.put(SHUFFLE_REMOTE_BLOCKS_FETCHED, 1000L)
    update.getShuffleReadMetrics.put(SHUFFLE_LOCAL_BLOCKS_FETCHED, 2000L)
    update.getShuffleReadMetrics.put(SHUFFLE_FETCH_WAIT_TIME, 3000L)
    update.getShuffleReadMetrics.put(SHUFFLE_REMOTE_BYTES_READ, 4000L)
    update.getShuffleReadMetrics.put(SHUFFLE_LOCAL_BYTES_READ, 5000L)
    update.getShuffleReadMetrics.put(SHUFFLE_TOTAL_RECORDS_READ, 6000L)

    update.getShuffleWriteMetrics.put(SHUFFLE_BYTES_WRITTEN, 18L)
    update.getShuffleWriteMetrics.put(SHUFFLE_WRITE_TIME, 19L)
    update.getShuffleWriteMetrics.put(SHUFFLE_RECORDS_WRITTEN, 20L)

    update.getInputMetrics.put(INPUT_BYTES_READ, 21L)
    update.getInputMetrics.put(INPUT_RECORDS_READ, 22L)

    update.getOutputMetrics.put(OUTPUT_BYTES_WRITTEN, 23L)
    update.getOutputMetrics.put(OUTPUT_RECORDS_WRITTEN, 24L)

    metrics.merge(update)
    metrics should be (update)
  }

  test("Merge non-empty metrics with non-empty update") {
    import com.github.lightcopy.history.model.Metrics._

    val metrics = new Metrics()
    metrics.setResultSize(1L)
    metrics.setJvmGcTime(2L)
    metrics.setResultSerializationTime(3L)
    metrics.setMemoryBytesSpilled(4L)
    metrics.setDiskBytesSpilled(5L)
    metrics.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_TIME, 6L)
    metrics.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, 7L)
    metrics.getExecutorMetrics.put(EXECUTOR_RUN_TIME, 8L)
    metrics.getExecutorMetrics.put(EXECUTOR_CPU_TIME, 9L)

    val update = new Metrics()
    update.setResultSize(10L)
    update.setJvmGcTime(20L)
    update.setResultSerializationTime(30L)
    update.setMemoryBytesSpilled(40L)
    update.setDiskBytesSpilled(50L)
    update.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_TIME, 60L)
    update.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, 70L)
    update.getExecutorMetrics.put(EXECUTOR_RUN_TIME, 80L)
    update.getExecutorMetrics.put(EXECUTOR_CPU_TIME, 90L)

    val exp = new Metrics()
    exp.setResultSize(11L)
    exp.setJvmGcTime(22L)
    exp.setResultSerializationTime(33L)
    exp.setMemoryBytesSpilled(44L)
    exp.setDiskBytesSpilled(55L)
    exp.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_TIME, 66L)
    exp.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, 77L)
    exp.getExecutorMetrics.put(EXECUTOR_RUN_TIME, 88L)
    exp.getExecutorMetrics.put(EXECUTOR_CPU_TIME, 99L)

    metrics.merge(update)
    metrics should be (exp)
  }

  test("Empty Stage into bson") {
    val stage = new Stage()
    val doc = serialize(stage, stage)
    val res = deserialize(stage, doc)
    res.getAppId should be (stage.getAppId)
    res.getJobId should be (stage.getJobId)
    res.getUniqueStageId should be (stage.getUniqueStageId)
    res.getStageId should be (stage.getStageId)
    res.getStageAttemptId should be (stage.getStageAttemptId)
    res.getStageName should be (stage.getStageName)
    res.getActiveTasks should be (stage.getActiveTasks)
    res.getCompletedTasks should be (stage.getCompletedTasks)
    res.getFailedTasks should be (stage.getFailedTasks)
    res.getTotalTasks should be (stage.getTotalTasks)
    res.getDetails should be (stage.getDetails)
    res.getStartTime should be (stage.getStartTime)
    res.getEndTime should be (stage.getEndTime)
    res.getDuration should be (stage.getDuration)
    res.getStatus should be (stage.getStatus)
    res.getMetrics should be (stage.getMetrics)
    res.getErrorDescription should be (stage.getErrorDescription)
    res.getErrorDetails should be (stage.getErrorDetails)
  }

  test("Complete Stage into bson") {
    val stage = new Stage()
    stage.setAppId("app")
    stage.setJobId(1)
    stage.setUniqueStageId(100L)
    stage.setStageId(1)
    stage.setStageAttemptId(2)
    stage.setStageName("stage")
    stage.setActiveTasks(10)
    stage.setCompletedTasks(12)
    stage.setFailedTasks(4)
    stage.setTotalTasks(26)
    stage.setDetails("details")
    stage.setStartTime(12L)
    stage.setEndTime(23L)
    stage.setDuration(4L)
    stage.setStatus(Stage.Status.FAILED)
    stage.setMetrics(new Metrics())
    stage.setErrorDescription("reason")
    stage.setErrorDetails("details")

    val doc = serialize(stage, stage)
    val res = deserialize(stage, doc)
    res.getAppId should be ("app")
    res.getJobId should be (1)
    res.getUniqueStageId should be (100L)
    res.getStageId should be (1)
    res.getStageAttemptId should be (2)
    res.getStageName should be ("stage")
    res.getActiveTasks should be (10)
    res.getCompletedTasks should be (12)
    res.getFailedTasks should be (4)
    res.getTotalTasks should be (26)
    res.getDetails should be ("details")
    res.getStartTime should be (12L)
    res.getEndTime should be (23L)
    res.getDuration should be (4L)
    res.getStatus should be (Stage.Status.FAILED)
    res.getMetrics should be (new Metrics())
    res.getErrorDescription should be ("reason")
    res.getErrorDetails should be ("details")
  }

  test("Stage from StageInfo") {
    val stage = new Stage()
    val info = new StageInfo()
    info.stageId = 1
    info.stageAttemptId = 2
    info.stageName = "stage"
    info.numTasks = 123
    info.details = "details"
    info.submissionTime = 100L
    info.completionTime = 202L
    info.failureReason = "reason\ndetails"

    stage.update(info)
    stage.getUniqueStageId should be (1L << 32 | 2L)
    stage.getStageId should be (1)
    stage.getStageAttemptId should be (2)
    stage.getStageName should be ("stage")
    stage.getTotalTasks should be (123)
    stage.getDetails should be ("details")
    stage.getStartTime should be (100L)
    stage.getEndTime should be (202L)
    stage.getDuration should be (102L)
    stage.getErrorDescription should be ("reason")
    stage.getErrorDetails should be ("reason\ndetails")
  }

  test("StageAggregateTracker - getters on empty obj") {
    val obj = AggregateSummary.stages()
    obj.getActiveTasks(1, 1) should be (0)
    obj.getCompletedTasks(2, 1) should be (0)
    obj.getFailedTasks(3, 1) should be (0)
    obj.getMetrics(4, 1) should be (new Metrics())
  }

  test("StageAggregateTracker - increment for stages") {
    val obj = AggregateSummary.stages()

    obj.incActiveTasks(1, 0)
    obj.incCompletedTasks(1, 0)
    obj.decActiveTasks(1, 0)

    obj.incActiveTasks(2, 0)
    obj.incFailedTasks(2, 0)
    obj.decActiveTasks(2, 0)

    val metrics = new Metrics()
    metrics.setResultSize(123L)
    obj.updateMetrics(2, 0, metrics)

    obj.getActiveTasks(1, 0) should be (0)
    obj.getCompletedTasks(1, 0) should be (1)
    obj.getFailedTasks(1, 0) should be (0)
    obj.getMetrics(1, 0) should be (new Metrics())

    obj.getActiveTasks(2, 0) should be (0)
    obj.getCompletedTasks(2, 0) should be (0)
    obj.getFailedTasks(2, 0) should be (1)
    obj.getMetrics(2, 0) should be (metrics)
  }

  test("StageAggregateTracker - increment for stages, test id") {
    val obj = AggregateSummary.stages()
    obj.incActiveTasks(10, 0)
    obj.incActiveTasks(0, 10)
    obj.getActiveTasks(10, 0) should be (1)
    obj.getActiveTasks(0, 10) should be (1)
  }

  test("StageAggregateTracker - equals") {
    val obj1 = AggregateSummary.stages()
    val obj2 = AggregateSummary.stages()
    obj1 should be (obj2)

    obj1.incActiveTasks(1, 0)
    obj1.incCompletedTasks(1, 0)
    obj1.incFailedTasks(1, 0)

    obj2.incActiveTasks(1, 0)
    obj2.incCompletedTasks(1, 0)
    obj2.incFailedTasks(1, 0)

    obj1 should be (obj2)
  }

  test("Empty Job to bson") {
    val job = new Job()
    val doc = serialize(job, job)
    val res = deserialize(job, doc)

    res.getAppId should be (job.getAppId)
    res.getJobId should be (job.getJobId)
    res.getJobName should be (job.getJobName)
    res.getStartTime should be (job.getStartTime)
    res.getEndTime should be (job.getEndTime)
    res.getDuration should be (job.getDuration)
    res.getStatus should be (job.getStatus)
    res.getErrorDescription should be (job.getErrorDescription)
    res.getErrorDetails should be (job.getErrorDetails)
    res.getActiveTasks should be (job.getActiveTasks)
    res.getCompletedTasks should be (job.getCompletedTasks)
    res.getFailedTasks should be (job.getFailedTasks)
    res.getTotalTasks should be (job.getTotalTasks)
    res.getMetrics should be (job.getMetrics)
  }

  test("Complete Job to bson") {
    val job = new Job()
    job.setAppId("app-123")
    job.setJobId(2)
    job.setJobName("name")
    job.setStartTime(123L)
    job.setEndTime(234L)
    job.setDuration(111L)
    job.setStatus(Job.Status.SUCCEEDED)
    job.setErrorDescription("")
    job.setErrorDetails(null)
    job.setActiveTasks(10)
    job.setCompletedTasks(20)
    job.setFailedTasks(30)
    job.setTotalTasks(60)
    job.setMetrics(new Metrics())

    val doc = serialize(job, job)
    val res = deserialize(job, doc)

    res.getAppId should be ("app-123")
    res.getJobId should be (2)
    res.getJobName should be ("name")
    res.getStartTime should be (123L)
    res.getEndTime should be (234L)
    res.getDuration should be (111L)
    res.getStatus should be (Job.Status.SUCCEEDED)
    res.getErrorDescription should be ("")
    res.getErrorDetails should be (null)
    res.getActiveTasks should be (10)
    res.getCompletedTasks should be (20)
    res.getFailedTasks should be (30)
    res.getTotalTasks should be (60)
    res.getMetrics should be (new Metrics())
  }

  test("Job update duration") {
    val job = new Job()

    job.updateDuration()
    job.getDuration should be (-1L)

    job.setStartTime(123L)
    job.updateDuration()
    job.getDuration should be (-1L)

    job.setStartTime(-1L)
    job.setEndTime(123L)
    job.updateDuration()
    job.getDuration should be (-1L)

    job.setStartTime(100L)
    job.setEndTime(300L)
    job.updateDuration()
    job.getDuration should be (200L)
  }
}
