/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.thrift;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ThriftHelper {
  private final List<FieldData> fields;

  public ThriftHelper(Class<? extends TBase> thriftClass) {
    Map<? extends TFieldIdEnum, FieldMetaData> mdm = FieldMetaData.getStructMetaDataMap(thriftClass);
    this.fields = Lists.newArrayListWithExpectedSize(mdm.size());
    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> e : mdm.entrySet()) {
      fields.add(new FieldData(e.getKey(), e.getValue()));
    }
  }

  public int getNumFields() {
    return fields.size();
  }

  public Object getFieldValue(int i, TBase tBase) {
    return tBase.getFieldValue(fields.get(i).id);
  }

  private static final Map<Byte, Class> TYPE_CLASSES = ImmutableMap.<Byte, Class>builder()
      .put(TType.BOOL, Boolean.class)
      .put(TType.DOUBLE, Double.class)
      .put(TType.I16, Integer.class)
      .put(TType.I32, Integer.class)
      .put(TType.I64, Long.class)
      .put(TType.STRING, String.class)
      .build();

  private static RelDataType getRelType(FieldMetaData metadata, RelDataTypeFactory typeFactory) {
    byte type = metadata.valueMetaData.type;
    if (TYPE_CLASSES.containsKey(type)) {
      return typeFactory.createJavaType(TYPE_CLASSES.get(type));
    } else {
      throw new UnsupportedOperationException("Unsupported Thrift type: " + type);
    }
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();
    for (FieldData fd : fields) {
      names.add(fd.id.getFieldName().toUpperCase(Locale.ENGLISH));
      types.add(getRelType(fd.metadata, typeFactory));
    }
    return typeFactory.createStructType(types, names);
  }

  private static class FieldData {
    TFieldIdEnum id;
    FieldMetaData metadata;

    public FieldData(TFieldIdEnum id, FieldMetaData metadata) {
      this.id = id;
      this.metadata = metadata;
    }
  }
}
