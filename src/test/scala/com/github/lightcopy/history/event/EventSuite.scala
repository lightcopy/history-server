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

package com.github.lightcopy.history.event

import scala.collection.JavaConverters._

import com.google.gson.Gson

import com.github.lightcopy.testutil.UnitTestSuite

class EventSuite extends UnitTestSuite {
  val gson = new Gson()

  test("Event") {
    val json1 = """
    {
      "Event": "SparkListenerApplicationStart",
      "App Name": "Spark shell",
      "App ID": "local-1497733035840"
    }
    """
    var event = gson.fromJson(json1, classOf[Event])
    event.getEventName() should be ("SparkListenerApplicationStart")

    val json2 = """
    {
      "App Name": "Spark shell",
      "App ID": "local-1497733035840",
      "Event": "TestEvent"
    }
    """
    event = gson.fromJson(json2, classOf[Event])
    event.getEventName() should be ("TestEvent")
  }

  test("SparkListenerEnvironmentUpdate") {
    val json = """
    {
      "Event": "SparkListenerEnvironmentUpdate",
      "JVM Information": {
        "Java Home": "/jdk1.7.0_80.jdk/Contents/Home/jre",
        "Java Version": "1.7.0_80 (Oracle Corporation)",
        "Scala Version": "version 2.11.8"
      },
      "Spark Properties": {
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+PrintGCDetails",
        "spark.eventLog.enabled": "true",
        "spark.driver.port": "49268"
      },
      "System Properties": {
        "sun.management.compiler": "HotSpot 64-Bit Tiered Compilers",
        "SPARK_SUBMIT": "true",
        "java.specification.version": "1.7",
        "java.version": "1.7.0_80"
      },
      "Classpath Entries": {
        "/tmp/spark-2.1.1-bin-hadoop2.7/jars/netty-all-4.0.42.Final.jar": "System Classpath",
        "/tmp/spark-2.1.1-bin-hadoop2.7/jars/spark-core_2.11-2.1.1.jar": "System Classpath",
        "/tmp/spark-2.1.1-bin-hadoop2.7/jars/spark-hive_2.11-2.1.1.jar": "System Classpath"
      }
    }
    """

    val event = gson.fromJson(json, classOf[SparkListenerEnvironmentUpdate])
    event.jvmInformation.asScala should be (Map(
      "Java Home" -> "/jdk1.7.0_80.jdk/Contents/Home/jre",
      "Java Version" -> "1.7.0_80 (Oracle Corporation)",
      "Scala Version" -> "version 2.11.8"
    ))

    event.sparkProperties.asScala should be (Map(
      "spark.executor.extraJavaOptions" -> "-XX:+UseG1GC -XX:+PrintGCDetails",
      "spark.eventLog.enabled" -> "true",
      "spark.driver.port" -> "49268"
    ))

    event.systemProperties.asScala should be (Map(
      "sun.management.compiler" -> "HotSpot 64-Bit Tiered Compilers",
      "SPARK_SUBMIT" -> "true",
      "java.specification.version" -> "1.7",
      "java.version" -> "1.7.0_80"
    ))

    event.classpathEntries.asScala should be (Map(
      "/tmp/spark-2.1.1-bin-hadoop2.7/jars/netty-all-4.0.42.Final.jar" -> "System Classpath",
      "/tmp/spark-2.1.1-bin-hadoop2.7/jars/spark-core_2.11-2.1.1.jar" -> "System Classpath",
      "/tmp/spark-2.1.1-bin-hadoop2.7/jars/spark-hive_2.11-2.1.1.jar" -> "System Classpath"
    ))
  }

  test("SparkListenerApplicationStart") {
    val json = """
    {
      "Event": "SparkListenerApplicationStart",
      "App Name": "Spark shell",
      "App ID": "local-1497733035840",
      "Timestamp": 1497733033849,
      "User": "sadikovi"
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerApplicationStart])
    event.appName should be ("Spark shell")
    event.appId should be ("local-1497733035840")
    event.timestamp should be (1497733033849L)
    event.user should be ("sadikovi")
  }

  test("SparkListenerApplicationEnd") {
    val json = """
    {
      "Event": "SparkListenerApplicationEnd",
      "Timestamp": 1497733079367
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerApplicationEnd])
    event.timestamp should be (1497733079367L)
  }

  test("SparkListenerSQLExecutionStart") {
    val json = """
    {
      "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
      "executionId": 1,
      "description": "count at <console>:24",
      "details": "org.apache.spark.sql.Dataset.count(Dataset.scala:2419)",
      "physicalPlanDescription": "== Parsed Logical Plan ==",
      "sparkPlanInfo": {
        "nodeName": "WholeStageCodegen",
        "simpleString": "WholeStageCodegen",
        "children": [
          {
            "nodeName": "HashAggregate",
            "simpleString": "HashAggregate(keys=[], functions=[count(1)])",
            "children": [],
            "metadata": {},
            "metrics": [
              {
                "name": "number of output rows",
                "accumulatorId": 369,
                "metricType": "sum"
              },
              {
                "name": "peak memory total (min, med, max)",
                "accumulatorId": 370,
                "metricType": "size"
              }
            ]
          }
        ],
        "metadata": {},
        "metrics": [
          {
            "name": "duration total (min, med, max)",
            "accumulatorId": 368,
            "metricType": "timing"
          }
        ]
      },
      "time": 1498724267295
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerSQLExecutionStart])
    event.executionId should be (1)
    event.description should be ("count at <console>:24")
    event.details should be ("org.apache.spark.sql.Dataset.count(Dataset.scala:2419)")
    event.physicalPlanDescription should be ("== Parsed Logical Plan ==")
    event.time should be (1498724267295L)
  }

  test("SparkListenerSQLExecutionEnd") {
    val json = """
    {
      "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
      "executionId": 1,
      "time": 1498350386321
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerSQLExecutionEnd])
    event.executionId should be (1)
    event.time should be (1498350386321L)
  }

  test("SparkListenerTaskStart") {
    val json = """
    {
      "Event": "SparkListenerTaskStart",
      "Stage ID": 1,
      "Stage Attempt ID": 0,
      "Task Info": {
        "Task ID": 14,
        "Index": 10,
        "Attempt": 0,
        "Launch Time": 1498420024582,
        "Executor ID": "0",
        "Host": "192.168.2.5",
        "Locality": "NODE_LOCAL",
        "Speculative": false,
        "Getting Result Time": 0,
        "Finish Time": 0,
        "Failed": false,
        "Killed": false,
        "Accumulables": []
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerTaskStart])
    event.stageId should be (1)
    event.stageAttemptId should be (0)
    val taskInfo = event.taskInfo
    taskInfo.taskId should be (14)
    taskInfo.index should be (10)
    taskInfo.attempt should be (0)
    taskInfo.launchTime should be (1498420024582L)
    taskInfo.executorId should be ("0")
    taskInfo.host should be ("192.168.2.5")
    taskInfo.locality should be ("NODE_LOCAL")
    taskInfo.speculative should be (false)
    taskInfo.gettingResultTime should be (0L)
    taskInfo.finishTime should be (0L)
    taskInfo.failed should be (false)
    taskInfo.killed should be (false)
    taskInfo.accumulables.size should be (0)
  }

  test("SparkListenerTaskEnd with FAILED status") {
    val json = """
    {
      "Event": "SparkListenerTaskEnd",
      "Stage ID": 1,
      "Stage Attempt ID": 1,
      "Task Type": "ResultTask",
      "Task End Reason": {
        "Reason": "ExceptionFailure",
        "Class Name": "java.lang.Exception",
        "Description": "Test",
        "Stack Trace": [
          {
            "Declaring Class": "scala.sys.package$",
            "Method Name": "error",
            "File Name": "package.scala",
            "Line Number": 27
          }
        ],
        "Full Stack Trace": "java.lang.Exception: Test\n\tat package$.error(package.scala:27)",
        "Accumulator Updates": [
          {
            "ID": 4,
            "Name": "internal.metrics.executorRunTime",
            "Update": 299,
            "Internal": true,
            "Count Failed Values": true
          },
          {
            "ID": 6,
            "Name": "internal.metrics.resultSize",
            "Update": 0,
            "Internal": true,
            "Count Failed Values": true
          }
        ]
      },
      "Task Info": {
        "Task ID": 2,
        "Index": 2,
        "Attempt": 0,
        "Launch Time": 1498724175390,
        "Executor ID": "driver",
        "Host": "localhost",
        "Locality": "PROCESS_LOCAL",
        "Speculative": false,
        "Getting Result Time": 0,
        "Finish Time": 1498724175778,
        "Failed": true,
        "Killed": false,
        "Accumulables": [
          {
            "ID": 4,
            "Name": "internal.metrics.executorRunTime",
            "Update": 299,
            "Value": 299,
            "Internal": true,
            "Count Failed Values": true
          }
        ]
      },
      "Task Metrics": {
        "Executor Deserialize Time": 0,
        "Executor Deserialize CPU Time": 0,
        "Executor Run Time": 299,
        "Executor CPU Time": 0,
        "Result Size": 0,
        "JVM GC Time": 0,
        "Result Serialization Time": 0,
        "Memory Bytes Spilled": 0,
        "Disk Bytes Spilled": 0,
        "Shuffle Read Metrics": {
          "Remote Blocks Fetched": 0,
          "Local Blocks Fetched": 0,
          "Fetch Wait Time": 0,
          "Remote Bytes Read": 0,
          "Local Bytes Read": 0,
          "Total Records Read": 0
        },
        "Shuffle Write Metrics": {
          "Shuffle Bytes Written": 0,
          "Shuffle Write Time": 0,
          "Shuffle Records Written": 0
        },
        "Input Metrics": {
          "Bytes Read": 0,
          "Records Read": 0
        },
        "Output Metrics": {
          "Bytes Written": 0,
          "Records Written": 0
        },
        "Updated Blocks": []
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerTaskEnd])
    event.stageId should be (1)
    event.stageAttemptId should be (1)
    event.taskType should be ("ResultTask")

    val taskInfo = event.taskInfo
    taskInfo.taskId should be (2)
    taskInfo.index should be (2)
    taskInfo.attempt should be (0)
    taskInfo.launchTime should be (1498724175390L)
    taskInfo.executorId should be ("driver")
    taskInfo.host should be ("localhost")
    taskInfo.locality should be ("PROCESS_LOCAL")
    taskInfo.speculative should be (false)
    taskInfo.gettingResultTime should be (0L)
    taskInfo.finishTime should be (1498724175778L)
    taskInfo.failed should be (true)
    taskInfo.killed should be (false)
    taskInfo.accumulables.size should be (1)

    val taskEndReason = event.taskEndReason
    taskEndReason.reason should be ("ExceptionFailure")
    taskEndReason.className should be ("java.lang.Exception")
    taskEndReason.description should be ("Test")
    taskEndReason.fullStackTrace should be (
      "java.lang.Exception: Test\n\tat package$.error(package.scala:27)")

    val taskMetrics = event.taskMetrics
    taskMetrics.executorDeserializeTime should be (0L)
    taskMetrics.executorDeserializeCpuTime should be (0L)
    taskMetrics.executorRunTime should be (299L)
    taskMetrics.executorCpuTime should be (0L)
    taskMetrics.resultSize should be (0L)
    taskMetrics.jvmGcTime should be (0L)
    taskMetrics.resultSerializationTime should be (0L)
    taskMetrics.memoryBytesSpilled should be (0L)
    taskMetrics.diskBytesSpilled should be (0L)

    taskMetrics.shuffleReadMetrics.remoteBlocksFetched should be (0L)
    taskMetrics.shuffleReadMetrics.localBlocksFetched should be (0L)
    taskMetrics.shuffleReadMetrics.fetchWaitTime should be (0L)
    taskMetrics.shuffleReadMetrics.remoteBytesRead should be (0L)
    taskMetrics.shuffleReadMetrics.localBytesRead should be (0L)
    taskMetrics.shuffleReadMetrics.totalRecordsRead should be (0L)

    taskMetrics.shuffleWriteMetrics.shuffleBytesWritten should be (0L)
    taskMetrics.shuffleWriteMetrics.shuffleWriteTime should be (0L)
    taskMetrics.shuffleWriteMetrics.shuffleRecordsWritten should be (0L)

    taskMetrics.inputMetrics.bytesRead should be (0L)
    taskMetrics.inputMetrics.recordsRead should be (0L)

    taskMetrics.outputMetrics.bytesWritten should be (0L)
    taskMetrics.outputMetrics.recordsWritten should be (0L)

    taskMetrics.updatedBlocks.size should be (0)
  }

  test("SparkListenerTaskEnd with SUCCESS status") {
    val json = """
    {
      "Event": "SparkListenerTaskEnd",
      "Stage ID": 1,
      "Stage Attempt ID": 0,
      "Task Type": "ResultTask",
      "Task End Reason": {
        "Reason": "Success"
      },
      "Task Info": {
        "Task ID": 9,
        "Index": 5,
        "Attempt": 0,
        "Launch Time": 1498420024463,
        "Executor ID": "0",
        "Host": "192.168.2.5",
        "Locality": "NODE_LOCAL",
        "Speculative": false,
        "Getting Result Time": 0,
        "Finish Time": 1498420024588,
        "Failed": false,
        "Killed": false,
        "Accumulables": [
          {
            "ID": 128,
            "Name": "internal.metrics.executorDeserializeTime",
            "Update": 18,
            "Value": 595,
            "Internal": true,
            "Count Failed Values": true
          }
        ]
      },
      "Task Metrics": {
        "Executor Deserialize Time": 18,
        "Executor Deserialize CPU Time": 12326000,
        "Executor Run Time": 71,
        "Executor CPU Time": 12000000,
        "Result Size": 2574,
        "JVM GC Time": 0,
        "Result Serialization Time": 0,
        "Memory Bytes Spilled": 0,
        "Disk Bytes Spilled": 0,
        "Shuffle Read Metrics": {
          "Remote Blocks Fetched": 0,
          "Local Blocks Fetched": 2,
          "Fetch Wait Time": 0,
          "Remote Bytes Read": 0,
          "Local Bytes Read": 118,
          "Total Records Read": 2
        },
        "Shuffle Write Metrics": {
          "Shuffle Bytes Written": 0,
          "Shuffle Write Time": 0,
          "Shuffle Records Written": 0
        },
        "Input Metrics": {
          "Bytes Read": 0,
          "Records Read": 0
        },
        "Output Metrics": {
          "Bytes Written": 0,
          "Records Written": 0
        },
        "Updated Blocks": [
          {
            "Block ID": "rdd_5_5",
            "Status": {
              "Storage Level": {
                "Use Disk": false,
                "Use Memory": true,
                "Deserialized": true,
                "Replication": 1
              },
              "Memory Size": 272,
              "Disk Size": 0
            }
          }
        ]
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerTaskEnd])
    event.stageId should be (1)
    event.stageAttemptId should be (0)
    event.taskType should be ("ResultTask")

    val taskInfo = event.taskInfo
    taskInfo.taskId should be (9)
    taskInfo.index should be (5)
    taskInfo.attempt should be (0)
    taskInfo.launchTime should be (1498420024463L)
    taskInfo.executorId should be ("0")
    taskInfo.host should be ("192.168.2.5")
    taskInfo.locality should be ("NODE_LOCAL")
    taskInfo.speculative should be (false)
    taskInfo.gettingResultTime should be (0L)
    taskInfo.finishTime should be (1498420024588L)
    taskInfo.failed should be (false)
    taskInfo.killed should be (false)
    taskInfo.accumulables.size should be (1)

    val taskEndReason = event.taskEndReason
    taskEndReason.reason should be ("Success")

    val taskMetrics = event.taskMetrics
    taskMetrics.executorDeserializeTime should be (18L)
    taskMetrics.executorDeserializeCpuTime should be (12326000L)
    taskMetrics.executorRunTime should be (71L)
    taskMetrics.executorCpuTime should be (12000000L)
    taskMetrics.resultSize should be (2574L)
    taskMetrics.jvmGcTime should be (0L)
    taskMetrics.resultSerializationTime should be (0L)
    taskMetrics.memoryBytesSpilled should be (0L)
    taskMetrics.diskBytesSpilled should be (0L)

    taskMetrics.shuffleReadMetrics.remoteBlocksFetched should be (0L)
    taskMetrics.shuffleReadMetrics.localBlocksFetched should be (2L)
    taskMetrics.shuffleReadMetrics.fetchWaitTime should be (0L)
    taskMetrics.shuffleReadMetrics.remoteBytesRead should be (0L)
    taskMetrics.shuffleReadMetrics.localBytesRead should be (118L)
    taskMetrics.shuffleReadMetrics.totalRecordsRead should be (2L)

    taskMetrics.shuffleWriteMetrics.shuffleBytesWritten should be (0L)
    taskMetrics.shuffleWriteMetrics.shuffleWriteTime should be (0L)
    taskMetrics.shuffleWriteMetrics.shuffleRecordsWritten should be (0L)

    taskMetrics.inputMetrics.bytesRead should be (0L)
    taskMetrics.inputMetrics.recordsRead should be (0L)

    taskMetrics.outputMetrics.bytesWritten should be (0L)
    taskMetrics.outputMetrics.recordsWritten should be (0L)

    taskMetrics.updatedBlocks.size should be (1)
    taskMetrics.updatedBlocks.get(0).blockId should be ("rdd_5_5")
    taskMetrics.updatedBlocks.get(0).status.memorySize should be (272L)
    taskMetrics.updatedBlocks.get(0).status.diskSize should be (0L)
    // storage level
    taskMetrics.updatedBlocks.get(0).status.storageLevel.useDisk should be (false)
    taskMetrics.updatedBlocks.get(0).status.storageLevel.useMemory should be (true)
    taskMetrics.updatedBlocks.get(0).status.storageLevel.deserialized should be (true)
    taskMetrics.updatedBlocks.get(0).status.storageLevel.replication should be (1)
  }

  test("TaskEndReason - Success") {
    val reason = new TaskEndReason()
    reason.reason = "Success"
    reason.getDescription should be ("")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - FetchFailed") {
    val reason = new TaskEndReason()
    reason.reason = "FetchFailed"
    reason.blockManagerAddress = null
    reason.shuffleId = 1
    reason.mapId = 2
    reason.reduceId = 3
    reason.message = "Message"
    reason.getDescription should be (
      "FetchFailed(null, shuffleId=1, mapId=2, reduceId=3, message=\nMessage\n)")
    reason.getDetails should be (null)

    reason.blockManagerAddress = new BlockManagerId()
    reason.blockManagerAddress.executorId = "0"
    reason.blockManagerAddress.host = "1.2.3.4"
    reason.blockManagerAddress.port = 45320
    reason.getDescription should be ("FetchFailed(BlockManagerId(0, 1.2.3.4, 45320), " +
      "shuffleId=1, mapId=2, reduceId=3, message=\nMessage\n)")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - ExceptionFailure") {
    val reason = new TaskEndReason()
    reason.reason = "ExceptionFailure"
    reason.className = "java.lang.RuntimeException"
    reason.description = "Test"
    reason.fullStackTrace = "FST"
    reason.getDescription should be ("java.lang.RuntimeException: Test")
    reason.getDetails should be ("FST")
  }

  test("TaskEndReason - TaskCommitDenied") {
    val reason = new TaskEndReason()
    reason.reason = "TaskCommitDenied"
    reason.jobId = 10
    reason.partitionId = 12
    reason.attemptNumber = 3
    reason.getDescription should be (
      "TaskCommitDenied (Driver denied task commit) for job: 10, partition: 12, attemptNumber: 3")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - TaskKilled") {
    val reason = new TaskEndReason()
    reason.reason = "TaskKilled"
    reason.killReason = "Test"
    reason.getDescription should be ("TaskKilled (Test)")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - ExecutorLostFailure") {
    val reason = new TaskEndReason()
    reason.reason = "ExecutorLostFailure"
    reason.executorId = "1"
    reason.causedByApp = true
    reason.lossReason = "Test"
    reason.getDescription should be (
      "ExecutorLostFailure (executor 1 exited caused by one of the running tasks) Reason: Test")
    reason.getDetails should be (null)

    reason.causedByApp = false
    reason.getDescription should be (
      "ExecutorLostFailure (executor 1 exited unrelated to the running tasks) Reason: Test")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - TaskResultLost") {
    val reason = new TaskEndReason()
    reason.reason = "TaskResultLost"
    reason.getDescription should be ("TaskResultLost (result lost from block manager)")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - Resubmitted") {
    val reason = new TaskEndReason()
    reason.reason = "Resubmitted"
    reason.getDescription should be ("Resubmitted (resubmitted due to lost executor)")
    reason.getDetails should be (null)
  }

  test("TaskEndReason - Unknown") {
    val reason = new TaskEndReason()
    reason.reason = "Unknown"
    reason.getDescription should be ("Unknown reason")
    reason.getDetails should be (null)
  }

  test("AccumulableInfo - parse primitive values") {
    val json = """
    {
      "ID": 4,
      "Name": "internal.metrics.executorDeserializeTime",
      "Update": 311,
      "Value": 311,
      "Internal": true,
      "Count Failed Values": true
    }
    """
    val event = gson.fromJson(json, classOf[AccumulableInfo])
    event.id should be (4)
    event.name should be ("internal.metrics.executorDeserializeTime")
    event.update should be (311)
    event.value should be (311)
  }

  test("AccumulableInfo - parse complex values") {
    val json = """
    {
      "ID": 14,
      "Name": "internal.metrics.updatedBlockStatuses",
      "Update": [
        {
          "Block ID": "rdd_3_0",
          "Status": {
            "Storage Level": {
              "Use Disk": false,
              "Use Memory": true,
              "Deserialized": true,
              "Replication": 1
            },
            "Memory Size": 392,
            "Disk Size": 0
          }
        }
      ],
      "Value": [
        {
          "Block ID": "rdd_3_0",
          "Status": {
            "Storage Level": {
              "Use Disk": false,
              "Use Memory": true,
              "Deserialized": true,
              "Replication": 1
            },
            "Memory Size": 392,
            "Disk Size": 0
          }
        }
      ],
      "Internal": true,
      "Count Failed Values": true
    }
    """
    val event = gson.fromJson(json, classOf[AccumulableInfo])
    event.id should be (14)
    event.name should be ("internal.metrics.updatedBlockStatuses")
    val exp = List(Map(
      "Block ID" -> "rdd_3_0",
      "Status" -> Map(
        "Storage Level" -> Map(
          "Use Disk" -> false,
          "Use Memory" -> true,
          "Deserialized" -> true,
          "Replication" -> 1.0
        ).asJava,
        "Memory Size" -> 392.0,
        "Disk Size" -> 0.0
      ).asJava
    ).asJava).asJava

    event.update should be (exp)
    event.value should be (exp)
  }

  test("SparkListenerStageSubmitted") {
    val json = """
    {
      "Event": "SparkListenerStageSubmitted",
      "Stage Info": {
        "Stage ID": 1,
        "Stage Attempt ID": 2,
        "Stage Name": "count at <console>:26",
        "Number of Tasks": 8,
        "RDD Info": [
          {
            "RDD ID": 8,
            "Name": "MapPartitionsRDD",
            "Scope": "{\"id\":\"15\",\"name\":\"Exchange\"}",
            "Callsite": "count at <console>:26",
            "Parent IDs": [
              7
            ],
            "Storage Level": {
              "Use Disk": false,
              "Use Memory": false,
              "Deserialized": false,
              "Replication": 1
            },
            "Number of Partitions": 8,
            "Number of Cached Partitions": 0,
            "Memory Size": 0,
            "Disk Size": 0
          }
        ],
        "Parent IDs": [],
        "Details": "org.apache.spark.sql.Dataset.count(Dataset.scala:2404)",
        "Accumulables": []
      },
      "Properties": {
        "spark.rdd.scope.noOverride": "true",
        "spark.rdd.scope": "{\"id\":\"21\",\"name\":\"collect\"}",
        "spark.sql.execution.id": "1"
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerStageSubmitted])
    event.stageInfo.stageId should be (1)
    event.stageInfo.stageAttemptId should be (2)
    event.stageInfo.stageName should be ("count at <console>:26")
    event.stageInfo.numTasks should be (8)
    event.stageInfo.parentIds.size should be (0)
    event.stageInfo.details should be ("org.apache.spark.sql.Dataset.count(Dataset.scala:2404)")
    event.stageInfo.submissionTime should be (0)
    event.stageInfo.completionTime should be (0)
    event.stageInfo.failureReason should be (null)
    event.properties should be (Map(
      "spark.rdd.scope.noOverride" -> "true",
      "spark.rdd.scope" -> "{\"id\":\"21\",\"name\":\"collect\"}",
      "spark.sql.execution.id" -> "1"
    ).asJava)
  }

  test("SparkListenerStageCompleted") {
    val json = """
    {
      "Event": "SparkListenerStageCompleted",
      "Stage Info": {
        "Stage ID": 1,
        "Stage Attempt ID": 2,
        "Stage Name": "collect at <console>:26",
        "Number of Tasks": 4,
        "RDD Info": [
          {
            "RDD ID": 10,
            "Name": "MapPartitionsRDD",
            "Scope": "{\"id\":\"8\",\"name\":\"mapPartitions\"}",
            "Callsite": "rdd at <console>:23",
            "Parent IDs": [
              9
            ],
            "Storage Level": {
              "Use Disk": false,
              "Use Memory": false,
              "Deserialized": false,
              "Replication": 1
            },
            "Number of Partitions": 4,
            "Number of Cached Partitions": 0,
            "Memory Size": 0,
            "Disk Size": 0
          }
        ],
        "Parent IDs": [],
        "Details": "org.apache.spark.rdd.RDD.collect(RDD.scala:935)",
        "Submission Time": 1498724228488,
        "Completion Time": 1498724228533,
        "Failure Reason": "Job aborted due to stage failure: Task 1 in stage 1.0 failed 1 times",
        "Accumulables": [
          {
            "ID": 126,
            "Name": "internal.metrics.executorRunTime",
            "Value": 10,
            "Internal": true,
            "Count Failed Values": true
          }
        ]
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerStageCompleted])
    event.stageInfo.stageId should be (1)
    event.stageInfo.stageAttemptId should be (2)
    event.stageInfo.stageName should be ("collect at <console>:26")
    event.stageInfo.numTasks should be (4)
    event.stageInfo.parentIds.size should be (0)
    event.stageInfo.details should be ("org.apache.spark.rdd.RDD.collect(RDD.scala:935)")
    event.stageInfo.submissionTime should be (1498724228488L)
    event.stageInfo.completionTime should be (1498724228533L)
    event.stageInfo.failureReason should be (
      "Job aborted due to stage failure: Task 1 in stage 1.0 failed 1 times")
  }

  test("SparkListenerJobStart") {
    val json = """
    {
      "Event": "SparkListenerJobStart",
      "Job ID": 2,
      "Submission Time": 1499039418340,
      "Stage Infos": [
        {
          "Stage ID": 1,
          "Stage Attempt ID": 2,
          "Stage Name": "collect at <console>:25",
          "Number of Tasks": 8,
          "RDD Info": [],
          "Parent IDs": [],
          "Details": "org.apache.spark.rdd.RDD.collect(RDD.scala:934)",
          "Accumulables": []
        }
      ],
      "Stage IDs": [
        0
      ],
      "Properties": {
        "spark.rdd.scope.noOverride": "true",
        "spark.rdd.scope": "{\"id\":\"2\",\"name\":\"collect\"}"
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerJobStart])
    event.jobId should be (2)
    event.submissionTime should be (1499039418340L)
    event.stageInfos.size should be (1)
    event.stageInfos.get(0).stageId should be (1)
    event.stageInfos.get(0).stageAttemptId should be (2)
  }

  test("SparkListenerJobEnd - success") {
    val json = """
    {
      "Event": "SparkListenerJobEnd",
      "Job ID": 2,
      "Completion Time": 1499038179014,
      "Job Result": {
        "Result": "JobSucceeded"
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerJobEnd])
    event.jobId should be (2)
    event.completionTime should be (1499038179014L)
    event.jobResult.result should be ("JobSucceeded")
    event.jobResult.isSuccess should be (true)
    event.jobResult.getDescription should be ("")
    event.jobResult.getDetails should be (null)
  }

  test("SparkListenerJobEnd - failure") {
    val json = """
    {
      "Event": "SparkListenerJobEnd",
      "Job ID": 2,
      "Completion Time": 1499039443514,
      "Job Result": {
        "Result": "JobFailed",
        "Exception": {
          "Message": "Job aborted due to stage failure: Task 3 in stage 0.0 failed 1 times\n",
          "Stack Trace": [
            {
              "Declaring Class": "org.apache.spark.scheduler.DAGScheduler",
              "Method Name": "org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages",
              "File Name": "DAGScheduler.scala",
              "Line Number": 1435
            },
            {
              "Declaring Class": "org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1",
              "Method Name": "apply",
              "File Name": "DAGScheduler.scala",
              "Line Number": 1423
            }
          ]
        }
      }
    }
    """
    val event = gson.fromJson(json, classOf[SparkListenerJobEnd])
    event.jobId should be (2)
    event.completionTime should be (1499039443514L)
    event.jobResult.result should be ("JobFailed")
    event.jobResult.isSuccess should be (false)
    event.jobResult.exception.message should be (
      "Job aborted due to stage failure: Task 3 in stage 0.0 failed 1 times\n")
    event.jobResult.getDescription should be (
      "Job aborted due to stage failure: Task 3 in stage 0.0 failed 1 times")
    event.jobResult.getDetails should be (
      "Job aborted due to stage failure: Task 3 in stage 0.0 failed 1 times\n")
  }
}
