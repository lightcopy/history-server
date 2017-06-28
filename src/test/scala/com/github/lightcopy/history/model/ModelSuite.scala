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
}
