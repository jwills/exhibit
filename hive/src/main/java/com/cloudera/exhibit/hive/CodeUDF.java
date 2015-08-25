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
package com.cloudera.exhibit.hive;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;

public abstract class CodeUDF extends GenericUDF {

  private String engine;

  protected CodeUDF(String engine) {
    this.engine = engine;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) {
      throw new UDFArgumentLengthException("Code UDFs take exactly one argument");
    }
    ObjectInspector codeOI = args[0];
    if (!ObjectInspectorUtils.isConstantObjectInspector(codeOI)) {
      throw new UDFArgumentException("Code argument must be a constant value");
    }
    String code;
    Object codeValue = ObjectInspectorUtils.getWritableConstantValue(codeOI);
    if (codeOI instanceof StringObjectInspector) {
      code = codeValue.toString();
    } else {
      ListObjectInspector lcoi = (ListObjectInspector) codeOI;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < lcoi.getListLength(codeValue); i++) {
        sb.append(lcoi.getListElement(codeValue, i)).append('\n');
      }
      code = sb.toString();
    }
    List<String> ret = Lists.newArrayList(engine, code);
    return ObjectInspectorFactory.getStandardConstantListObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector, ret);
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    return Lists.newArrayList(engine, args[0].get());
  }
}
