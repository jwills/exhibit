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

import com.google.common.collect.Lists
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericData}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.junit.{Before, Test, Assert}

class ExhibitRDDTest {

  val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
  val sc = new SQLContext(new SparkContext(conf))
  val innerSchema = SchemaBuilder.record("SparkInTest").fields()
      .optionalDouble("a").optionalString("b").optionalInt("c")
      .endRecord()
  val outerSchema = SchemaBuilder.record("SparkExTest").fields()
      .optionalString("name")
      .name("tbl").`type`().array().items(innerSchema).arrayDefault(Lists.newArrayList())
      .endRecord()
  var testFile: File = null

  @Before def setUp: Unit = {
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

  @Test def readExhibit: Unit = {
    val erdd = ExhibitRDD.avroFile(testFile.toString, sc)
    val defcnt = erdd.sql("""select sum(a) suma, sum(c) sumc, count(*) from tbl""")
    val res = defcnt.collect()
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(32.0, res(0).getDouble(0), 0.001)
    Assert.assertEquals(46, res(0).getInt(1))
    Assert.assertEquals(2L, res(0).getLong(2))
  }
}
