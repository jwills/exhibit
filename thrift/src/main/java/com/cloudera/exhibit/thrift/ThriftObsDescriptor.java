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
package com.cloudera.exhibit.thrift;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ThriftObsDescriptor implements ObsDescriptor {
  private final List<Field> fields;
  private final List<Short> ids;
  private final Map<String, Integer> fieldNames;

  public ThriftObsDescriptor(Class<? extends TBase> thriftClass) {
    Map<? extends TFieldIdEnum, FieldMetaData> mdm = FieldMetaData.getStructMetaDataMap(thriftClass);
    this.fields = Lists.newArrayListWithExpectedSize(mdm.size());
    this.ids = Lists.newArrayListWithExpectedSize(mdm.size());
    this.fieldNames = Maps.newHashMap();
    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> e : mdm.entrySet()) {
      fields.add(new Field(e.getValue().fieldName, getFieldType(e.getValue())));
      ids.add(e.getKey().getThriftFieldId());
      fieldNames.put(e.getValue().fieldName, fields.size() - 1);
    }
  }

  @Override
  public int size() {
    return fields.size();
  }

  Object getFieldValue(final int i, TBase tBase) {
    return tBase.getFieldValue(new TFieldIdEnum() {
      @Override
      public short getThriftFieldId() {
        return ids.get(i);
      }
      @Override
      public String getFieldName() {
        return fields.get(i).name;
      }
    });
  }

  private static final Map<Byte, FieldType> TYPE_CLASSES = ImmutableMap.<Byte, FieldType>builder()
      .put(TType.BOOL, FieldType.BOOLEAN)
      .put(TType.DOUBLE, FieldType.DOUBLE)
      .put(TType.I16, FieldType.INTEGER)
      .put(TType.I32, FieldType.INTEGER)
      .put(TType.I64, FieldType.LONG)
      .put(TType.STRING, FieldType.STRING)
      .build();

  private static FieldType getFieldType(FieldMetaData metadata) {
    byte type = metadata.valueMetaData.type;
    if (TYPE_CLASSES.containsKey(type)) {
      return TYPE_CLASSES.get(type);
    } else {
      throw new UnsupportedOperationException("Unsupported Thrift type: " + type);
    }
  }

  @Override
  public Field get(int i) {
    return fields.get(i);
  }

  @Override
  public int indexOf(String name) {
    Integer idx = fieldNames.get(name);
    if (idx == null) {
      return -1;
    }
    return idx;
  }

  @Override
  public Iterator<Field> iterator() {
    return fields.iterator();
  }
}
