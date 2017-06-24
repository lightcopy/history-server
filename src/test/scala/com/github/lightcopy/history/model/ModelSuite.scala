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

import scala.collection.JavaConverters._

import org.bson.{BsonDocument, BsonDocumentWriter, BsonDocumentReader}
import org.bson.codecs.{DecoderContext, EncoderContext}

import com.github.lightcopy.testutil.UnitTestSuite

class ModelSuite extends UnitTestSuite {
  test("Empty application to Bson") {
    val app = new Application()

    val doc = new BsonDocument()
    val writer = new BsonDocumentWriter(doc)
    val encoder = EncoderContext.builder().build()
    app.encode(writer, app, encoder)

    val reader = new BsonDocumentReader(doc)
    val decoder = DecoderContext.builder().build()
    val res = app.decode(reader, decoder)

    res.getId() should be (app.getId())
    res.getName() should be (app.getName())
    res.getStartTime() should be (app.getStartTime())
    res.getEndTime() should be (app.getEndTime())
    res.getUser() should be (app.getUser())
    res.getJvmInformation() should be (app.getJvmInformation())
    res.getSparkProperties() should be (app.getSparkProperties())
    res.getSystemProperties() should be (app.getSystemProperties())
    res.getClasspathEntries() should be (app.getClasspathEntries())
  }

  test("Complete application to Bson") {
    val app = new Application()
    app.setId("app-id")
    app.setName("app-name")
    app.setStartTime(1000L)
    app.setEndTime(2000L)
    app.setUser("user")
    app.setJvmInformation(Map("a.1" -> "b.1", "a.2" -> "b.2").asJava)
    app.setSparkProperties(Map("c.1" -> "d.1", "c.2" -> "d.2").asJava)
    app.setSystemProperties(Map("e.1" -> "f.1", "e.2" -> "f.2").asJava)
    app.setClasspathEntries(Map("g.1" -> "h.1", "g.2" -> "h.2").asJava)

    val doc = new BsonDocument()
    val writer = new BsonDocumentWriter(doc)
    val encoder = EncoderContext.builder().build()
    app.encode(writer, app, encoder)

    val reader = new BsonDocumentReader(doc)
    val decoder = DecoderContext.builder().build()
    val res = app.decode(reader, decoder)

    res.getId() should be ("app-id")
    res.getName() should be ("app-name")
    res.getStartTime() should be (1000L)
    res.getEndTime() should be (2000L)
    res.getUser() should be ("user")
    res.getJvmInformation() should be (Map("a.1" -> "b.1", "a.2" -> "b.2").asJava)
    res.getSparkProperties() should be (Map("c.1" -> "d.1", "c.2" -> "d.2").asJava)
    res.getSystemProperties() should be (Map("e.1" -> "f.1", "e.2" -> "f.2").asJava)
    res.getClasspathEntries() should be (Map("g.1" -> "h.1", "g.2" -> "h.2").asJava)
  }
}
