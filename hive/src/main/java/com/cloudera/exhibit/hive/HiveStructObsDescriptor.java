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

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Iterator;
import java.util.List;

class HiveStructObsDescriptor extends HiveObsDescriptor {
  private final StructObjectInspector obji;

  public HiveStructObsDescriptor(StructObjectInspector obji) {
    this.obji = obji;
  }

  @Override
  public Field get(int i) {
    StructField sf = obji.getAllStructFieldRefs().get(i);
    return new Field(sf.getFieldName(), HiveUtils.getFieldType(sf.getFieldObjectInspector()));
  }

  @Override
  public int indexOf(String name) {
    return obji.getAllStructFieldRefs().indexOf(obji.getStructFieldRef(name));
  }

  @Override
  public int size() {
    return obji.getAllStructFieldRefs().size();
  }

  @Override
  public ObsDescriptor clone() {
    return new HiveStructObsDescriptor(this.obji);
  }

  @Override
  public Iterator<Field> iterator() {
    return Iterators.transform(obji.getAllStructFieldRefs().iterator(), new Function<StructField, Field>() {
      @Override
      public Field apply(StructField structField) {
        return new Field(structField.getFieldName(), HiveUtils.getFieldType(structField.getFieldObjectInspector()));
      }
    });
  }

  @Override
  public Object[] convert(Object rawObs) {
    List v = Lists.newArrayListWithExpectedSize(obji.getAllStructFieldRefs().size());
    ObjectInspectorUtils.copyToStandardObject(v, rawObs, obji, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
    for (int i = 0; i < v.size(); i++) {
      v.set(i, HiveUtils.asJavaType(v.get(i)));
    }
    return v.toArray();
  }
}
