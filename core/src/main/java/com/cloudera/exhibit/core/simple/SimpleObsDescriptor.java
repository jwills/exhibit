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
package com.cloudera.exhibit.core.simple;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Joiner;
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

  public static Builder builder() {
    return new Builder();
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

  @Override
  public int hashCode() {
    return fields.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof SimpleObsDescriptor)) {
      return false;
    }
    SimpleObsDescriptor sod = (SimpleObsDescriptor) other;
    return fields.equals(sod.fields);
  }

  @Override
  public String toString() {
    return Joiner.on(", ").join(fields);
  }

  public static class Builder {
    private List<Field> fields = Lists.newArrayList();

    public Builder add(String name, FieldType ft) {
      fields.add(new Field(name, ft));
      return this;
    }

    public Builder booleanField(String name) {
      return add(name, FieldType.BOOLEAN);
    }

    public Builder intField(String name) {
      return add(name, FieldType.INTEGER);
    }

    public Builder longField(String name) {
      return add(name, FieldType.LONG);
    }

    public Builder floatField(String name) {
      return add(name, FieldType.FLOAT);
    }

    public Builder doubleField(String name) {
      return add(name, FieldType.DOUBLE);
    }

    public Builder decimalField(String name) {
      return add(name, FieldType.DECIMAL);
    }

    public Builder dateField(String name) {
      return add(name, FieldType.DATE);
    }

    public Builder timestampField(String name) {
      return add(name, FieldType.TIMESTAMP);
    }

    public Builder stringField(String name) {
      return add(name, FieldType.STRING);
    }

    public ObsDescriptor build() {
      return new SimpleObsDescriptor(fields);
    }
  }
}
