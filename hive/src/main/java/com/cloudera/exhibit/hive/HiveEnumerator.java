/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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
import org.apache.calcite.linq4j.Enumerator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.List;

class HiveEnumerator implements Enumerator<Object> {

  private final Object obj;
  private final ListObjectInspector listOI;
  private final ObjectInspector elOI;
  private final int size;
  private int currentIndex = -1;
  private Object currentValue;

  public HiveEnumerator(Object obj, ListObjectInspector listOI) {
    this.obj = obj;
    this.listOI = listOI;
    this.elOI = listOI.getListElementObjectInspector();
    this.size = listOI.getListLength(obj);
    this.currentValue = null;
  }

  @Override
  public Object current() {
    return currentValue;
  }

  @Override
  public boolean moveNext() {
    currentIndex++;
    boolean hasNext = currentIndex < size;
    if (hasNext) {
      updateValues();
    }
    return hasNext;
  }

  @Override
  public void reset() {
    currentIndex = -1;
  }

  @Override
  public void close() {
  }

  private void updateValues() {
    Object row = listOI.getListElement(obj, currentIndex);
    if (elOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      currentValue = ((PrimitiveObjectInspector) elOI).getPrimitiveJavaObject(row);
      currentValue = HiveUtils.asJavaType(currentValue);
    } else {
      List v = Lists.newArrayList();
      ObjectInspectorUtils.copyToStandardObject(v, row, (StructObjectInspector) elOI,
          ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
      for (int i = 0; i < v.size(); i++) {
        v.set(i, HiveUtils.asJavaType(v.get(i)));
      }
      currentValue = v.toArray();
    }
  }
}
