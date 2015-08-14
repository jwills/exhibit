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

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Vec;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.Iterator;

public class HiveVector implements Vec {

  private final FieldType fieldType;
  private final ListObjectInspector listOI;
  private final PrimitiveObjectInspector pOI;
  private Object values;

  public HiveVector(FieldType fieldType, ListObjectInspector listOI) {
    this.fieldType = fieldType;
    this.listOI = listOI;
    this.pOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
  }

  @Override
  public FieldType getType() {
    return fieldType;
  }

  @Override
  public Object get(int index) {
    Object v = listOI.getListElement(values, index);
    return HiveUtils.asJavaType(pOI.getPrimitiveJavaObject(v));
  }

  @Override
  public int size() {
    return listOI.getListLength(values);
  }

  @Override
  public Iterator<Object> iterator() {
    //TODO
    return null;
  }

  public HiveVector updateValues(Object values) {
    this.values = values;
    return this;
  }
}
