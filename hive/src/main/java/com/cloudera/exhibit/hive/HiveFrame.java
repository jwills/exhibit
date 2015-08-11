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

import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Iterator;

public class HiveFrame extends Frame {

  private final HiveObsDescriptor descriptor;
  private final ListObjectInspector listOI;
  private Object values;

  public HiveFrame(ListObjectInspector listOI) {
    this.listOI = listOI;
    ObjectInspector elOI = listOI.getListElementObjectInspector();
    if (elOI instanceof StructObjectInspector) {
      this.descriptor = new HiveStructObsDescriptor((StructObjectInspector) elOI);
    } else {
      this.descriptor = new HivePrimitiveObsDescriptor((PrimitiveObjectInspector) elOI);
    }
  }

  public HiveFrame updateValues(Object values) {
    this.values = values;
    return this;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    if (values == null) {
      return 0;
    }
    return listOI.getListLength(values);
  }

  @Override
  public Obs get(int index) {
    return new HiveObs(descriptor, listOI.getListElement(values, index));
  }

  @Override
  public Iterator<Obs> iterator() {
    return null;
  }
}
