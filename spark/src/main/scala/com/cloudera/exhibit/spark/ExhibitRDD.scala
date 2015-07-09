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

import java.io.FileNotFoundException

import com.cloudera.exhibit.avro.AvroExhibit
import com.cloudera.exhibit.core.ObsDescriptor.FieldType
import com.cloudera.exhibit.core._
import com.cloudera.exhibit.javascript.JSCalculator
import com.cloudera.exhibit.sql.SQLCalculator

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.fs.{AvroFSInput, FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

class ExhibitRDD private[spark](
    @transient val sqlContext: SQLContext,
    @transient val descriptor: ExhibitDescriptor,
    val parent: RDD[Exhibit]) extends RDD[Exhibit](parent) {
  import ExhibitRDD._
  import scala.collection.JavaConversions.asScalaIterator

  @transient private val schemas = exhibitDescriptor2schemas(descriptor)
  @transient private var attrsDF: DataFrame = null
  @transient private val frameDFs = new scala.collection.mutable.HashMap[String, DataFrame]()

  def attrs(): DataFrame = {
    if (attrsDF == null) {
      attrsDF = sqlContext.createDataFrame(parent.map(e => obs2row(e.attributes())), schemas._1)
    }
    return attrsDF
  }

  def frames(): Iterator[String] = {
    descriptor.frames.keySet.iterator
  }

  def frame(name: String): DataFrame = {
    if (!frameDFs.contains(name)) {
      if (!descriptor.frames().containsKey(name)) {
        return sqlContext.emptyDataFrame
      }
      val rows = parent.flatMap(e => e.frames().get(name).iterator().map(obs => obs2row(obs)))
      frameDFs += name -> sqlContext.createDataFrame(rows, schemas._2(name))
    }
    frameDFs(name)
  }

  def calculate(calc: Calculator): DataFrame = {
    val schema = obsDescriptor2schema(calc.initialize(descriptor))
    val rows = parent.mapPartitions(e => applyCalc(e, calc))
    sqlContext.createDataFrame(rows, schema)
  }

  def sql(query: String): DataFrame = sql(Array(query))

  def sql(queries: Array[String]): DataFrame = {
    calculate(new SQLCalculator(queries))
  }

  def js(code: String): DataFrame = calculate(new JSCalculator(code))

  def js(od: ObsDescriptor, code: String): DataFrame = calculate(new JSCalculator(od, code))

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Exhibit] = {
    parent.compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = parent.partitions
}

object ExhibitRDD {
  import scala.collection.JavaConversions.{asScalaIterator, mapAsScalaMap}

  def avroFile(path: String, sc: SQLContext): ExhibitRDD = avroFile(new Path(path), sc)

  def avroFile(path: Path, sc: SQLContext): ExhibitRDD = {
    val reader = newAvroReader(path, sc)
    val schema = reader.getSchema
    reader.close()
    val descriptor = AvroExhibit.createDescriptor(schema)
    val parent = avroRDD(path, sc).map(record => AvroExhibit.create(record._1.datum()))
    return new ExhibitRDD(sc, descriptor, parent)
  }

  private val typeMap = Map[FieldType, Serializable](
    FieldType.BOOLEAN -> BooleanType,
    FieldType.DATE -> DateType,
    FieldType.DECIMAL -> DecimalType,
    FieldType.DOUBLE -> DoubleType,
    FieldType.FLOAT -> FloatType,
    FieldType.INTEGER -> IntegerType,
    FieldType.LONG -> LongType,
    FieldType.SHORT -> ShortType,
    FieldType.STRING -> StringType,
    FieldType.TIME -> TimestampType,
    FieldType.TIMESTAMP -> TimestampType)

  private def applyCalc(exhibits: Iterator[Exhibit], calc: Calculator): Iterator[Row] = {
    var init = false
    val ret = exhibits.flatMap(e => {
      if (!init) {
        calc.initialize(e.descriptor())
        init = true
      }
      calc.apply(e).iterator()
    }).map(obs => obs2row(obs))
    calc.cleanup()
    ret
  }

  private def exhibitDescriptor2schemas(ed: ExhibitDescriptor): (StructType, Map[String, StructType]) = {
    val attrSchema = obsDescriptor2schema(ed.attributes())
    val frameSchemas = ed.frames().toMap.mapValues(od => obsDescriptor2schema(od))
    (attrSchema, frameSchemas)
  }

  private def obs2row(obs: Obs) = Row(obs.iterator().toArray:_*)

  private def obsDescriptor2schema(od: ObsDescriptor): StructType = {
    new StructType(od.iterator().map(f => StructField(f.name, typeMap(f.`type`).asInstanceOf[DataType], true)).toArray)
  }

  private def avroRDD(path: Path, sqlContext: SQLContext) = {
    sqlContext.sparkContext.hadoopFile(
        path.toString,
        classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
        classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
        classOf[org.apache.hadoop.io.NullWritable])
  }

  private def newAvroReader(path: Path, sqlContext: SQLContext) = {
    val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(path.toUri, hadoopConfiguration)
    val globStatus = fs.globStatus(path)
    if (globStatus == null) {
      throw new FileNotFoundException(s"The path you've provided ($path) is invalid.")
    }
    val singleFile = globStatus.toStream.head
    val input = new FsInput(singleFile.getPath, hadoopConfiguration)
    val reader = new GenericDatumReader[GenericRecord]()
    DataFileReader.openReader(input, reader)
  }
}
