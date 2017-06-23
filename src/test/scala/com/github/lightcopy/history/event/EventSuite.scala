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
}
