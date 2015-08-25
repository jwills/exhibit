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

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.Arrays;
import java.util.List;

class HiveAttributes extends Obs {

  private final ObsDescriptor desc;
  private final List<PrimitiveObjectInspector> oi;
  private final List<Object> values;

  public HiveAttributes(ObsDescriptor desc, List<PrimitiveObjectInspector> pois) {
    super();
    this.desc = desc;
    this.oi = pois;
    this.values = Arrays.asList(new Object[pois.size()]);
  }

  void update(int index, Object value) {
    this.values.set(index, value);
  }

  @Override
  public ObsDescriptor descriptor() {
    return desc;
  }

  @Override
  public Object get(int index) {
    return oi.get(index).getPrimitiveJavaObject(values.get(index));
  }
}
