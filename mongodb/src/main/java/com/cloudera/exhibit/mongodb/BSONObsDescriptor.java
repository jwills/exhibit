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
package com.cloudera.exhibit.mongodb;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BSONObsDescriptor implements ObsDescriptor {

  private final List<String> names;
  private final List<FieldType> fieldTypes;
  private final List<String> columns;

  public BSONObsDescriptor(List<String> names, List<FieldType> fieldTypes) {
    this(names, fieldTypes, ImmutableMap.<String, String>of());
  }

  public BSONObsDescriptor(List<String> names, List<FieldType> fieldTypes, Map<String, String> mappings) {
    this.names = names;
    this.fieldTypes = fieldTypes;
    this.columns = getColumns(names, mappings);
  }

  private static List<String> getColumns(List<String> names, final Map<String, String> mappings) {
    List<String> ret = Lists.newArrayListWithExpectedSize(names.size());
    for (int i = 0; i < names.size(); i++) {
      String key = names.get(i);
      String col = mappings.get(key);
      ret.add(col == null ? key : col);
    }
    return ret;
  }

  @Override
  public Field get(int i) {
    return new Field(names.get(i), fieldTypes.get(i));
  }

  String getBSONColumn(int i) {
    return columns.get(i);
  }

  @Override
  public int indexOf(String name) {
    return names.indexOf(name);
  }

  @Override
  public int size() {
    return names.size();
  }

  @Override
  public Iterator<Field> iterator() {
    return new UnmodifiableIterator<Field>() {
      private int index = 0;
      @Override
      public boolean hasNext() {
        return index < size();
      }

      @Override
      public Field next() {
        Field f = get(index);
        index++;
        return f;
      }
    };
  }
}
