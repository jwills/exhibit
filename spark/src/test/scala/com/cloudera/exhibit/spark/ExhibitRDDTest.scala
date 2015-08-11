/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.exhibit.spark

import java.io.File

import com.cloudera.exhibit.core.Exhibit
import com.cloudera.exhibit.javascript.JsFunctorConstants
import com.cloudera.exhibit.sql.SQLFunctor
import com.google.common.collect.Lists
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Assert, Before, Test}

class ExhibitRDDTest {

  val innerSchema = SchemaBuilder.record("SparkInTest").fields()
    .optionalDouble("a").optionalString("b").optionalInt("c")
    .endRecord()
  val outerSchema = SchemaBuilder.record("SparkExTest").fields()
    .optionalString("name")
    .name("tbl").`type`().array().items(innerSchema).arrayDefault(Lists.newArrayList())
    .endRecord()

  var conf: SparkConf = null
  var sc: SQLContext = null
  var testFile: File = null

  @After def tearDown: Unit = {
    sc.sparkContext.stop
    sc = null
    // To avoid Akka rebinding to the same port,
    // since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  @Before def setUp: Unit = {
    conf = new SparkConf()
           .setMaster("local")
           .setAppName(getClass.getName)

    sc = new SQLContext(new SparkContext(conf))

    testFile = File.createTempFile("exhibit", ".avro")
    testFile.deleteOnExit()
    val writer = new GenericDatumWriter[GenericData.Record](outerSchema)
    val dfw = new DataFileWriter[GenericData.Record](writer).create(outerSchema, testFile)
    val one = new GenericData.Record(outerSchema)
    one.put("name", "josh")
    val r1 = new GenericData.Record(innerSchema)
    r1.put("a", 29.0)
    r1.put("b", "abc")
    r1.put("c", 17)
    val r2 = new GenericData.Record(innerSchema)
    r2.put("a", 3.0)
    r2.put("b", "xyz")
    r2.put("c", 29)
    val vals1 = Lists.newArrayList(r1, r2)
    one.put("tbl", vals1)

    dfw.append(one)
    dfw.close()
  }

  @Test def readExhibitSQL: Unit = {
    val erdd = ExhibitRDD.avroFile(testFile.toString, sc)
    val defcnt = erdd.sql( """select sum(a) suma, sum(c) sumc, count(*) from tbl""")
    val exhibits = defcnt.collect()
    Assert.assertEquals(1, exhibits.size)
    val resultExhibit: Exhibit = exhibits(0)
    Assert.assertTrue(resultExhibit.frames().containsKey(SQLFunctor.DEFAULT_RESULT_FRAME))
    val res = resultExhibit.frames.get(SQLFunctor.DEFAULT_RESULT_FRAME)
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(32.0, res.get(0).get(0))
    Assert.assertEquals(46, res.get(0).get(1))
    Assert.assertEquals(2L, res.get(0).get(2))
  }

  @Test def readExhibitJS: Unit = {
    val erdd = ExhibitRDD.avroFile(testFile.toString, sc)
    val defcnt = erdd.js("tbl.length")
    val exhibits = defcnt.collect()
    Assert.assertEquals(1, exhibits.size)
    val resultExhibit: Exhibit = exhibits(0)
    Assert.assertEquals(1, resultExhibit.attributes().size())
    val res = resultExhibit.attributes().get(JsFunctorConstants.DEFAULT_FIELD_NAME)
    Assert.assertEquals(2.0, res)
  }
}
