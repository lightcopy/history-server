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

  test("Create application") {
    val app = new Application()
    app.setAppId("app-id")
    app.setAppName("app-name")
    app.setStartTime(1000L)
    app.setEndTime(2000L)
    app.setUser("user")
    app.setInProgress(true)
    app.setPath("file:/tmp")
    app.setSize(128L)
    app.setModificationTime(3000L)
    app.setStatus(Application.Status.SUCCESS)

    app.getAppName() should be ("app-name")
    app.getAppId() should be ("app-id")
    app.getStartTime() should be (1000L)
    app.getEndTime() should be (2000L)
    app.getUser() should be ("user")
    app.inProgress() should be (true)
    app.getPath() should be ("file:/tmp")
    app.getSize() should be (128L)
    app.getModificationTime() should be (3000L)
    app.getStatus() should be (Application.Status.SUCCESS)
  }

  test("Create empty application") {
    val app = new Application()

    app.getAppId() should be (null)
    app.getAppName() should be (null)
    app.getStartTime() should be (-1L)
    app.getEndTime() should be (-1L)
    app.getUser() should be (null)
    app.inProgress() should be (false)
    app.getPath() should be (null)
    app.getSize() should be (0L)
    app.getModificationTime() should be (-1L)
    app.getStatus() should be (Application.Status.PROCESSING)
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
    res.inProgress() should be (app.inProgress())
    res.getPath() should be (app.getPath())
    res.getSize() should be (app.getSize())
    res.getModificationTime() should be (app.getModificationTime())
    res.getStatus() should be (app.getStatus())
  }

  test("Complete application to bson") {
    val app = new Application()
    app.setAppId("app-id")
    app.setAppName("app-name")
    app.setStartTime(1000L)
    app.setEndTime(2000L)
    app.setUser("user")
    app.setInProgress(true)
    app.setPath("file:/tmp")
    app.setSize(128L)
    app.setModificationTime(3000L)
    app.setStatus(Application.Status.SUCCESS)

    val doc = serialize(app, app)
    val res = deserialize(app, doc)

    res.getAppId() should be ("app-id")
    res.getAppName() should be ("app-name")
    res.getStartTime() should be (1000L)
    res.getEndTime() should be (2000L)
    res.getUser() should be ("user")
    app.inProgress() should be (true)
    app.getPath() should be ("file:/tmp")
    app.getSize() should be (128L)
    app.getModificationTime() should be (3000L)
    app.getStatus() should be (Application.Status.SUCCESS)
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
    val env = new Environment()
    env.setAppId("app-id")
    env.setJvmInformation(hm(Map("a.1" -> "b.1", "a.2" -> "b.2")))
    env.setSparkProperties(hm(Map("c.1" -> "d.1", "c.2" -> "d.2")))
    env.setSystemProperties(hm(Map("e.1" -> "f.1", "e.2" -> "f.2")))
    env.setClasspathEntries(hm(Map("g.1" -> "h.1", "g.2" -> "h.2")))

    val doc = serialize(env, env)
    val res = deserialize(env, doc)

    res.getAppId() should be ("app-id")
    res.getJvmInformation() should be (hm(Map("a.1" -> "b.1", "a.2" -> "b.2")))
    res.getSparkProperties() should be (hm(Map("c.1" -> "d.1", "c.2" -> "d.2")))
    res.getSystemProperties() should be (hm(Map("e.1" -> "f.1", "e.2" -> "f.2")))
    res.getClasspathEntries() should be (hm(Map("g.1" -> "h.1", "g.2" -> "h.2")))
  }
}
