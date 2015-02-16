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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.Iterator;

class HivePrimitiveObsDescriptor implements HiveObsDescriptor {

  private PrimitiveObjectInspector poi;

  public HivePrimitiveObsDescriptor(PrimitiveObjectInspector poi) {
    this.poi = poi;
  }

  @Override
  public Field get(int i) {
    return new Field("c1", HiveUtils.getFieldType(poi));
  }

  @Override
  public int indexOf(String name) {
    if ("c1".equals(name)) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public Iterator<Field> iterator() {
    return ImmutableList.of(get(0)).iterator();
  }

  @Override
  public Object[] convert(Object rawObs) {
    return new Object[] { HiveUtils.asJavaType(poi.getPrimitiveJavaObject(rawObs)) };
  }
}
