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

  def hm[T](map: Map[String, T]): java.util.HashMap[String, T] = {
    val res = new java.util.HashMap[String, T]()
    for ((key, value) <- map) {
      res.put(key, value)
    }
    res
  }

  def hs[T](values: T*): java.util.HashSet[T] = {
    val res = new java.util.HashSet[T]()
    for (value <- values) {
      res.add(value)
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
    res.getLoadProgress() should be (app.getLoadProgress())
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
    app.setLoadProgress(0.23)

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
    app.getLoadProgress() should be (0.23)
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
    task.getStatus should be (Task.Status.SUCCESS)
    task.getErrorDescription should be ("")
    task.getErrorDetails should be (null)

    reason.reason = "TaskKilled"
    reason.killReason = "Test"
    task.update(reason)
    task.getStatus should be (Task.Status.KILLED)
    task.getErrorDescription should be ("TaskKilled (Test)")
    task.getErrorDetails should be (null)

    reason.reason = "Resubmitted"
    task.update(reason)
    task.getStatus should be (Task.Status.FAILED)
    task.getErrorDescription should be ("Resubmitted (resubmitted due to lost executor)")
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
    taskMetrics.shuffleReadMetrics.fetchWaitTime = 14L * 1000000L
    taskMetrics.shuffleReadMetrics.remoteBytesRead = 15L
    taskMetrics.shuffleReadMetrics.localBytesRead = 16L
    taskMetrics.shuffleReadMetrics.totalRecordsRead = 17L

    taskMetrics.shuffleWriteMetrics = new TaskMetrics.ShuffleWriteMetrics()
    taskMetrics.shuffleWriteMetrics.shuffleBytesWritten = 18L
    taskMetrics.shuffleWriteMetrics.shuffleWriteTime = 19L * 1000000L
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

    res should be (Metrics.fromTaskMetrics(taskMetrics))
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

  test("Metrics copy") {
    val metrics = new Metrics()
    metrics.setResultSize(1L)
    metrics.setJvmGcTime(2L)
    val copy = metrics.copy()

    copy should be (metrics)

    metrics.setResultSize(100L)
    metrics.getResultSize should be (100L)
    copy.getResultSize should be (1L)
  }

  test("Metrics subtract") {
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
    exp.setResultSize(9L)
    exp.setJvmGcTime(18L)
    exp.setResultSerializationTime(27L)
    exp.setMemoryBytesSpilled(36L)
    exp.setDiskBytesSpilled(45L)
    exp.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_TIME, 54L)
    exp.getExecutorMetrics.put(EXECUTOR_DESERIALIZE_CPU_TIME, 63L)
    exp.getExecutorMetrics.put(EXECUTOR_RUN_TIME, 72L)
    exp.getExecutorMetrics.put(EXECUTOR_CPU_TIME, 81L)

    val delta = update.delta(metrics)
    delta should be (exp)
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
    res.getJobGroup should be (stage.getJobGroup)
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
    res.getTaskTime should be (stage.getTaskTime)
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
    stage.setJobGroup("group")
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
    stage.setTaskTime(123456L)
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
    res.getJobGroup should be ("group")
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
    res.getTaskTime should be (123456L)
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

  test("Stage - incremental task updates") {
    val stage = new Stage()
    stage.incActiveTasks()
    stage.incActiveTasks()
    stage.getActiveTasks() should be (2)

    stage.decActiveTasks()
    stage.getActiveTasks() should be (1)

    stage.incFailedTasks()
    stage.getFailedTasks() should be (1)

    stage.incCompletedTasks()
    stage.getCompletedTasks() should be (1)
  }

  test("Stage - increment task time") {
    val stage = new Stage()
    stage.getTaskTime should be (0L)
    stage.incTaskTime(100L)
    stage.incTaskTime(200L)
    stage.incTaskTime(-100L)
    stage.incTaskTime(0L)
    stage.getTaskTime should be (300L)
  }

  test("Empty Job to bson") {
    val job = new Job()
    val doc = serialize(job, job)
    val res = deserialize(job, doc)

    res.getAppId should be (job.getAppId)
    res.getJobId should be (job.getJobId)
    res.getJobName should be (job.getJobName)
    res.getJobGroup should be (job.getJobGroup)
    res.getStartTime should be (job.getStartTime)
    res.getEndTime should be (job.getEndTime)
    res.getDuration should be (job.getDuration)
    res.getStatus should be (job.getStatus)
    res.getErrorDescription should be (job.getErrorDescription)
    res.getErrorDetails should be (job.getErrorDetails)
    res.getActiveTasks should be (job.getActiveTasks)
    res.getCompletedTasks should be (job.getCompletedTasks)
    res.getFailedTasks should be (job.getFailedTasks)
    res.getSkippedTasks should be (job.getSkippedTasks)
    res.getTotalTasks should be (job.getTotalTasks)
    res.getMetrics should be (job.getMetrics)
    res.getPendingStages should be (job.getPendingStages)
    res.getActiveStages should be (job.getActiveStages)
    res.getCompletedStages should be (job.getCompletedStages)
    res.getFailedStages should be (job.getFailedStages)
    res.getSkippedStages should be (job.getSkippedStages)
  }

  test("Complete Job to bson") {
    val job = new Job()
    job.setAppId("app-123")
    job.setJobId(2)
    job.setJobName("name")
    job.setJobGroup("group")
    job.setStartTime(123L)
    job.setEndTime(234L)
    job.setDuration(111L)
    job.setStatus(Job.Status.SUCCEEDED)
    job.setErrorDescription("")
    job.setErrorDetails(null)
    job.setActiveTasks(10)
    job.setCompletedTasks(20)
    job.setFailedTasks(30)
    job.setSkippedTasks(40)
    job.setTotalTasks(60)
    job.setMetrics(new Metrics())
    job.setPendingStages(hs(100L))
    job.setActiveStages(hs(200L))
    job.setCompletedStages(hs(300L))
    job.setFailedStages(hs(400L))
    job.setSkippedStages(hs(500L))

    val doc = serialize(job, job)
    val res = deserialize(job, doc)

    res.getAppId should be ("app-123")
    res.getJobId should be (2)
    res.getJobName should be ("name")
    res.getJobGroup should be ("group")
    res.getStartTime should be (123L)
    res.getEndTime should be (234L)
    res.getDuration should be (111L)
    res.getStatus should be (Job.Status.SUCCEEDED)
    res.getErrorDescription should be ("")
    res.getErrorDetails should be (null)
    res.getActiveTasks should be (10)
    res.getCompletedTasks should be (20)
    res.getFailedTasks should be (30)
    res.getSkippedTasks should be (40)
    res.getTotalTasks should be (60)
    res.getMetrics should be (new Metrics())
    res.getPendingStages should be (hs(100L))
    res.getActiveStages should be (hs(200L))
    res.getCompletedStages should be (hs(300L))
    res.getFailedStages should be (hs(400L))
    res.getSkippedStages should be (hs(500L))
  }

  test("Job - incremental task updates") {
    val job = new Job()
    job.incActiveTasks()
    job.incActiveTasks()
    job.getActiveTasks() should be (2)

    job.decActiveTasks()
    job.getActiveTasks() should be (1)

    job.incFailedTasks()
    job.getFailedTasks() should be (1)

    job.incCompletedTasks()
    job.getCompletedTasks() should be (1)

    job.incSkippedTasks(11)
    job.incSkippedTasks(4)
    job.getSkippedTasks() should be (15)
  }

  test("Job - incremental stage updates") {
    val job = new Job()
    job.markStagePending(100, 1)
    job.getPendingStages() should be (hs(100.toLong << 32 | 1))

    job.markStageActive(200, 1)
    job.getActiveStages() should be (hs(200.toLong << 32 | 1))

    job.markStageCompleted(300, 1)
    job.getCompletedStages() should be (hs(300.toLong << 32 | 1))

    job.markStageFailed(400, 1)
    job.getFailedStages() should be (hs(400.toLong << 32 | 1))

    job.markStageSkipped(500, 1)
    job.getSkippedStages() should be (hs(500.toLong << 32 | 1))
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

  test("Empty executor to bson") {
    val exc = new Executor()
    val doc = serialize(exc, exc)
    val res = deserialize(exc, doc)
    res.getAppId should be (exc.getAppId)
    res.getExecutorId should be (exc.getExecutorId)
    res.getSortExecutorId should be (exc.getSortExecutorId)
    res.getHost should be (exc.getHost)
    res.getPort should be (exc.getPort)
    res.getCores should be (exc.getCores)
    res.getMaxMemory should be (exc.getMaxMemory)
    res.getStartTime should be (exc.getStartTime)
    res.getEndTime should be (exc.getEndTime)
    res.getDuration should be (exc.getDuration)
    res.getStatus should be (exc.getStatus)
    res.getFailureReason should be (exc.getFailureReason)
    res.getLogs should be (exc.getLogs)
    res.getActiveTasks should be (exc.getActiveTasks)
    res.getCompletedTasks should be (exc.getCompletedTasks)
    res.getFailedTasks should be (exc.getFailedTasks)
    res.getTotalTasks should be (exc.getTotalTasks)
    res.getTaskTime should be (exc.getTaskTime)
    res.getMetrics should be (exc.getMetrics)
  }

  test("Complete executor to bson") {
    val exc = new Executor()
    exc.setAppId("app-123")
    exc.setExecutorId("executor-1")
    exc.setSortExecutorId(1)
    exc.setHost("host")
    exc.setPort(45323)
    exc.setCores(16)
    exc.setMaxMemory(1024L)
    exc.setStartTime(100L)
    exc.setEndTime(300L)
    exc.updateDuration()
    exc.setStatus(Executor.Status.REMOVED)
    exc.setFailureReason("Reason")
    exc.setLogs(hm(Map("stdout" -> "1", "stderr" -> "2")))
    exc.setActiveTasks(1000)
    exc.setCompletedTasks(2000)
    exc.setFailedTasks(3000)
    exc.setTotalTasks(6000)
    exc.setTaskTime(100000L)
    exc.setMetrics(new Metrics())

    val doc = serialize(exc, exc)
    val res = deserialize(exc, doc)

    res.getAppId should be ("app-123")
    res.getExecutorId should be ("executor-1")
    res.getSortExecutorId should be (1)
    res.getHost should be ("host")
    res.getPort should be (45323)
    res.getCores should be (16)
    res.getMaxMemory should be (1024L)
    res.getStartTime should be (100L)
    res.getEndTime should be (300L)
    res.getDuration should be (200L)
    res.getStatus should be (Executor.Status.REMOVED)
    res.getFailureReason should be ("Reason")
    res.getStdoutUrl should be ("1")
    res.getStderrUrl should be ("2")
    res.getActiveTasks should be (1000)
    res.getCompletedTasks should be (2000)
    res.getFailedTasks should be (3000)
    res.getTotalTasks should be (6000)
    res.getTaskTime should be (100000L)
    res.getMetrics should be (new Metrics())
  }

  test("Executor - updateDuration") {
    val exc = new Executor()

    exc.updateDuration()
    exc.getDuration should be (-1L)

    exc.setStartTime(123L)
    exc.updateDuration()
    exc.getDuration should be (-1L)

    exc.setStartTime(-1L)
    exc.setEndTime(123L)
    exc.updateDuration()
    exc.getDuration should be (-1L)

    exc.setStartTime(100L)
    exc.setEndTime(300L)
    exc.updateDuration()
    exc.getDuration should be (200L)
  }

  test("Executor - update sort id") {
    val exc = new Executor()
    exc.getSortExecutorId should be (Int.MaxValue)

    exc.setExecutorId("driver")
    exc.updateSortExecutorId()
    exc.getSortExecutorId should be (Int.MaxValue)

    exc.setExecutorId("10")
    exc.updateSortExecutorId()
    exc.getSortExecutorId should be (10)

    exc.setExecutorId("10abcd")
    exc.updateSortExecutorId()
    exc.getSortExecutorId should be (Int.MaxValue)
  }

  test("Executor - task updates") {
    val exc = new Executor()
    exc.incActiveTasks()
    exc.incCompletedTasks()
    exc.incFailedTasks()
    exc.incTaskTime(100L)
    exc.incTaskTime(20L)

    exc.getActiveTasks should be (1)
    exc.getCompletedTasks should be (1)
    exc.getFailedTasks should be (1)
    exc.getTotalTasks should be (3)
    exc.getTaskTime should be (120L)
  }

  test("Empty ApplicationSummary to bson") {
    val sum = new ApplicationSummary()
    val doc = serialize(sum, sum)
    val res = deserialize(sum, doc)
    res.getRunningJobs should be (sum.getRunningJobs)
    res.getSucceededJobs should be (sum.getSucceededJobs)
    res.getFailedJobs should be (sum.getFailedJobs)
    res.getActiveExecutors should be (sum.getActiveExecutors)
    res.getRemovedExecutors should be (sum.getRemovedExecutors)
  }

  test("Complete ApplicationSummary to bson") {
    val sum = new ApplicationSummary()
    val jobSummary = new ApplicationSummary.JobSummary()
    jobSummary.jobId = 1
    jobSummary.pendingStages = 2;
    jobSummary.activeStages = 3;
    jobSummary.completedStages = 4;
    jobSummary.failedStages = 5;
    jobSummary.skippedStages = 6;

    sum.setRunningJobs(hm(Map("1" -> jobSummary)))
    sum.setSucceededJobs(hm(Map("1" -> jobSummary)))
    sum.setFailedJobs(hm(Map("1" -> jobSummary)))

    sum.setActiveExecutors(hs("a", "b", "c"))
    sum.setRemovedExecutors(hs("d", "e"))

    sum.setRunningQueries(23)
    sum.setCompletedQueries(45)

    val doc = serialize(sum, sum)
    val res = deserialize(sum, doc)

    res.getRunningJobs should be (sum.getRunningJobs)
    res.getSucceededJobs should be (sum.getSucceededJobs)
    res.getFailedJobs should be (sum.getFailedJobs)
    res.getActiveExecutors should be (sum.getActiveExecutors)
    res.getRemovedExecutors should be (sum.getRemovedExecutors)
    res.getRunningQueries should be (sum.getRunningQueries)
    res.getCompletedQueries should be (sum.getCompletedQueries)
  }

  test("ApplicationSummary - upsert job") {
    val sum = new ApplicationSummary()
    val job = new Job()
    job.setJobId(12)

    job.setStatus(Job.Status.RUNNING)
    job.markStagePending(1, 2)
    job.markStageActive(2, 3)

    sum.update(job)
    sum.getRunningJobs().size should be (1)
    sum.getSucceededJobs().size should be (0)
    sum.getFailedJobs().size should be (0)
    sum.getRunningJobs.get("12").jobId should be (12)
    sum.getRunningJobs.get("12").pendingStages should be (1)
    sum.getRunningJobs.get("12").activeStages should be (1)
    sum.getRunningJobs.get("12").completedStages should be (0)
    sum.getRunningJobs.get("12").failedStages should be (0)
    sum.getRunningJobs.get("12").skippedStages should be (0)

    job.markStageCompleted(2, 3)
    job.markStageFailed(3, 4)

    sum.update(job)
    sum.getRunningJobs().size should be (1)
    sum.getSucceededJobs().size should be (0)
    sum.getFailedJobs().size should be (0)
    sum.getRunningJobs.get("12").jobId should be (12)
    sum.getRunningJobs.get("12").pendingStages should be (1)
    sum.getRunningJobs.get("12").activeStages should be (0)
    sum.getRunningJobs.get("12").completedStages should be (1)
    sum.getRunningJobs.get("12").failedStages should be (1)
    sum.getRunningJobs.get("12").skippedStages should be (0)

    job.setStatus(Job.Status.SUCCEEDED)

    sum.update(job)
    sum.getRunningJobs().size should be (0)
    sum.getSucceededJobs().size should be (1)
    sum.getFailedJobs().size should be (0)
    sum.getSucceededJobs.get("12").jobId should be (12)
    sum.getSucceededJobs.get("12").pendingStages should be (1)
    sum.getSucceededJobs.get("12").activeStages should be (0)
    sum.getSucceededJobs.get("12").completedStages should be (1)
    sum.getSucceededJobs.get("12").failedStages should be (1)
    sum.getSucceededJobs.get("12").skippedStages should be (0)
  }

  test("ApplicationSummary - upsert executor") {
    val sum = new ApplicationSummary()
    val exc = new Executor()
    exc.setExecutorId("123")

    sum.update(exc)
    sum.getActiveExecutors.size should be (0)
    sum.getRemovedExecutors.size should be (0)

    exc.setStatus(Executor.Status.ACTIVE)
    sum.update(exc)
    sum.getActiveExecutors.size should be (1)
    sum.getRemovedExecutors.size should be (0)

    exc.setStatus(Executor.Status.REMOVED)
    sum.update(exc)
    sum.getActiveExecutors.size should be (0)
    sum.getRemovedExecutors.size should be (1)
  }

  test("ApplicationSummary - upsert query") {
    val sum = new ApplicationSummary()
    val sql = new SQLExecution()

    sum.update(sql)
    sum.getRunningQueries should be (0)
    sum.getCompletedQueries should be (0)

    sql.setStatus(SQLExecution.Status.RUNNING)
    sum.update(sql)
    sum.getRunningQueries should be (1)
    sum.getCompletedQueries should be (0)

    sql.setStatus(SQLExecution.Status.COMPLETED)
    sum.update(sql)
    sum.getRunningQueries should be (0)
    sum.getCompletedQueries should be (1)
  }

  test("Empty StageSummary to bson") {
    val sum = new StageSummary()
    val doc = serialize(sum, sum)
    val res = deserialize(sum, doc)

    res.getAppId should be (sum.getAppId)
    res.getStageId should be (sum.getStageId)
    res.getStageAttemptId should be (sum.getStageAttemptId)
    res.stageMetrics.numTasks should be (sum.stageMetrics.numTasks)
    res.stageMetrics.taskDuration should be (sum.stageMetrics.taskDuration)
    res.stageMetrics.taskDeserializationTime should be (sum.stageMetrics.taskDeserializationTime)
    res.stageMetrics.gcTime should be (sum.stageMetrics.gcTime)
    res.stageMetrics.resultSerializationTime should be (sum.stageMetrics.resultSerializationTime)
    res.stageMetrics.shuffleFetchWaitTime should be (sum.stageMetrics.shuffleFetchWaitTime)
    res.stageMetrics.shuffleRemoteBytesRead should be (sum.stageMetrics.shuffleRemoteBytesRead)
    res.stageMetrics.shuffleLocalBytesRead should be (sum.stageMetrics.shuffleLocalBytesRead)
    res.stageMetrics.shuffleTotalRecordsRead should be (sum.stageMetrics.shuffleTotalRecordsRead)
    res.stageMetrics.shuffleBytesWritten should be (sum.stageMetrics.shuffleBytesWritten)
    res.stageMetrics.shuffleWriteTime should be (sum.stageMetrics.shuffleWriteTime)
    res.stageMetrics.shuffleRecordsWritten should be (sum.stageMetrics.shuffleRecordsWritten)
    res.executors should be (sum.executors)
  }

  test("Complete StageSummary to bson") {
    val sum = new StageSummary()
    sum.setAppId("app-123")
    sum.setStageId(23)
    sum.setStageAttemptId(67)
    sum.stageMetrics.numTasks = 3
    sum.stageMetrics.taskDuration =
      StageSummary.MetricPercentiles.fromArray(Array[Long](1, 2, 3, 4, 5))
    sum.stageMetrics.shuffleRemoteBytesRead =
      StageSummary.MetricPercentiles.fromArray(Array[Long](1, 2, 3))
    sum.stageMetrics.shuffleRecordsWritten =
      StageSummary.MetricPercentiles.fromArray(Array[Long](1, 2, 3))
    sum.executors.put("0", new StageSummary.ExecutorMetrics())
    sum.executors.get("0").taskTime = 123L
    sum.executors.get("0").totalTasks = 2L
    sum.executors.get("0").runningTasks = 1L
    sum.executors.get("0").succeededTasks = 1L

    val doc = serialize(sum, sum)
    val res = deserialize(sum, doc)

    res.getAppId should be ("app-123")
    res.getStageId should be (23)
    res.getStageAttemptId should be (67)
    res.stageMetrics.numTasks should be (3)
    res.stageMetrics.taskDuration should be (sum.stageMetrics.taskDuration)
    res.stageMetrics.taskDeserializationTime should be (sum.stageMetrics.taskDeserializationTime)
    res.stageMetrics.gcTime should be (sum.stageMetrics.gcTime)
    res.stageMetrics.resultSerializationTime should be (sum.stageMetrics.resultSerializationTime)
    res.stageMetrics.shuffleFetchWaitTime should be (sum.stageMetrics.shuffleFetchWaitTime)
    res.stageMetrics.shuffleRemoteBytesRead should be (sum.stageMetrics.shuffleRemoteBytesRead)
    res.stageMetrics.shuffleLocalBytesRead should be (sum.stageMetrics.shuffleLocalBytesRead)
    res.stageMetrics.shuffleTotalRecordsRead should be (sum.stageMetrics.shuffleTotalRecordsRead)
    res.stageMetrics.shuffleBytesWritten should be (sum.stageMetrics.shuffleBytesWritten)
    res.stageMetrics.shuffleWriteTime should be (sum.stageMetrics.shuffleWriteTime)
    res.stageMetrics.shuffleRecordsWritten should be (sum.stageMetrics.shuffleRecordsWritten)
    res.executors should be (sum.executors)
  }

  test("StageSummary - setSummary") {
    val sum = new StageSummary()
    val task = new Task()
    task.setExecutorId("0")
    task.setDuration(123L)
    task.setStatus(Task.Status.SUCCESS)
    sum.setSummary(al(task :: task :: task :: Nil))
    sum.stageMetrics.numTasks should be (3)
    sum.stageMetrics.taskDuration should be (
      StageSummary.MetricPercentiles.fromArray(Array(123L, 123L, 123L)))
    sum.executors.size should be (1)
    sum.executors.get("0").taskTime should be (369L)
  }
}
