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
        "Class Name": "java.lang.RuntimeException",
        "Description": "Test failure",
        "Stack Trace": [
          {
            "Declaring Class": "scala.sys.package$",
            "Method Name": "error",
            "File Name": "package.scala",
            "Line Number": 27
          }
        ],
        "Full Stack Trace": "java.lang.RuntimeException: Test failure\n\tat scala.sys.package$.error(package.scala:27)",
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
    // check accumulable
    taskInfo.accumulables.get(0).id should be (4)
    taskInfo.accumulables.get(0).name should be ("internal.metrics.executorRunTime")
    taskInfo.accumulables.get(0).update should be ("299")
    taskInfo.accumulables.get(0).value should be ("299")

    val taskEndReason = event.taskEndReason
    taskEndReason.reason should be ("ExceptionFailure")
    taskEndReason.className should be ("java.lang.RuntimeException")
    taskEndReason.description should be ("Test failure")
    taskEndReason.fullStackTrace should be ("java.lang.RuntimeException: Test failure\n\tat " +
      "scala.sys.package$.error(package.scala:27)")

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
    // check accumulable
    taskInfo.accumulables.get(0).id should be (128)
    taskInfo.accumulables.get(0).name should be ("internal.metrics.executorDeserializeTime")
    taskInfo.accumulables.get(0).update should be ("18")
    taskInfo.accumulables.get(0).value should be ("595")

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
}
