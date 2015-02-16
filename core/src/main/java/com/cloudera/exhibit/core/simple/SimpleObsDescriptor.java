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
package com.cloudera.exhibit.core.simple;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleObsDescriptor implements ObsDescriptor {

  private final List<Field> fields;
  private final Map<String, Integer> indexMap;

  public static SimpleObsDescriptor of(String name, FieldType type, Object... args) {
    List<Field> fields = Lists.newArrayList();
    fields.add(new Field(name, type));
    for (int i = 0; i < args.length; i += 2) {
      fields.add(new Field(args[i].toString(), (FieldType) args[i + 1]));
    }
    return new SimpleObsDescriptor(fields);
  }

  public SimpleObsDescriptor(List<Field> fields) {
    this.fields = fields;
    this.indexMap = Maps.newHashMap();
    for (int i = 0; i < fields.size(); i++) {
      indexMap.put(fields.get(i).name, i);
    }
  }

  @Override
  public Field get(int i) {
    return fields.get(i);
  }

  @Override
  public int indexOf(String name) {
    Integer im = indexMap.get(name);
    return im == null ? -1 : im;
  }

  @Override
  public int size() {
    return fields.size();
  }

  @Override
  public Iterator<Field> iterator() {
    return fields.iterator();
  }
}
