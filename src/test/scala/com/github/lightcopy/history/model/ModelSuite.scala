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

  test("Filled application to Bson") {
    val app = new Application()
    app.setId("app-id")
    app.setName("app-name")
    app.setStartTime(1000L)
    app.setEndTime(2000L)
    app.setUser("user")
    app.setJvmInformation(Map("a" -> "b").asJava)
    app.setSparkProperties(Map("c" -> "d").asJava)
    app.setSystemProperties(Map("e" -> "f").asJava)
    app.setClasspathEntries(Map("g" -> "h").asJava)

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
    res.getJvmInformation() should be (Map("a" -> "b").asJava)
    res.getSparkProperties() should be (Map("c" -> "d").asJava)
    res.getSystemProperties() should be (Map("e" -> "f").asJava)
    res.getClasspathEntries() should be (Map("g" -> "h").asJava)
  }
}
