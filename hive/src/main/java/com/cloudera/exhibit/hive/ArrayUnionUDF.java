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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.List;
import java.util.Set;

public class ArrayUnionUDF extends GenericUDF {

  private List<ListObjectInspector> argOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length < 2) {
      throw new UDFArgumentException("Expecting at least two arguments to array_union");
    }
    this.argOIs = Lists.newArrayListWithExpectedSize(args.length);
    ObjectInspector elemOI = null;
    for (ObjectInspector oi : args) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      argOIs.add(loi);
      ObjectInspector eoi = ObjectInspectorUtils.getStandardObjectInspector(loi.getListElementObjectInspector());
      if (elemOI == null) {
        elemOI = eoi;
      } else if (!elemOI.equals(eoi)) {
        throw new UDFArgumentException("Array elements must all be of the same type");
      }
    }
    return ObjectInspectorFactory.getStandardListObjectInspector(elemOI);
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    Set<Object> distinct = Sets.newHashSet();
    for (int i = 0; i < args.length; i++) {
      ListObjectInspector loi = argOIs.get(i);
      List list = loi.getList(args[i].get());
      for (int j = 0; j < list.size(); j++) {
        distinct.add(ObjectInspectorUtils.copyToStandardObject(list.get(j), loi.getListElementObjectInspector()));
      }
    }
    return Lists.newArrayList(distinct);
  }

  @Override
  public String getDisplayString(String[] args) {
    assert (args.length > 1);
    return "array_union(" + Joiner.on(',').join(args) + ")";
  }
}
