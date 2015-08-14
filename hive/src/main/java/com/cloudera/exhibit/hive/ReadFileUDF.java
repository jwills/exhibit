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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ReadFileUDF extends GenericUDF {

  private List<String> contents;

  private static List<String> read(String fileName, String delim) throws IOException {
    List<String> contents = Files.readLines(new File(fileName), Charsets.UTF_8);
    if (!"\n".equals(delim)) {
      contents = Lists.newArrayList(Splitter.on(delim).split(Joiner.on("\n").join(contents)));
    }
    return contents;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length < 1 || args.length > 2) {
      throw new UDFArgumentLengthException("The read_file UDF takes at least 1 and no more than 2 args");
    }
    if (ObjectInspectorUtils.isConstantObjectInspector(args[0]) &&
        ObjectInspectorUtils.isConstantObjectInspector(args[1])) {
      String fileName = ObjectInspectorUtils.getWritableConstantValue(args[0]).toString();
      String delim = ObjectInspectorUtils.getWritableConstantValue(args[1]).toString();
      boolean success = true;
      try {
        loadContents(fileName, delim);
      } catch (Exception e) {
        success = false;
      }
      if (success) {
        return ObjectInspectorFactory.getStandardConstantListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, contents);
      }
    }
    return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

  private void loadContents(String fileName, String delim) throws HiveException {
    if (!fileName.contains("/")) {
      fileName = "./" + fileName;
    }
    try {
      contents = read(fileName, delim);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    if (contents == null) {
      loadContents(args[0].get().toString(), args[1].get().toString());
    }
    return contents;
  }

  @Override
  public String getDisplayString(String[] args) {
    assert (args.length > 1);
    return "read_file(" + Joiner.on(',').join(args) + ")";
  }
}
