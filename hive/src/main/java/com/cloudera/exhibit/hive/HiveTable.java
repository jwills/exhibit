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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HiveTable extends AbstractQueryableTable {

  private static final Map<Class, Class> typeMap = ImmutableMap.<Class, Class>builder()
      .put(BooleanObjectInspector.class, Boolean.class)
      .put(StringObjectInspector.class, String.class)
      .put(IntObjectInspector.class, Integer.class)
      .put(LongObjectInspector.class, Long.class)
      .put(DoubleObjectInspector.class, Double.class)
      .put(FloatObjectInspector.class, Float.class)
      .put(ByteObjectInspector.class, Byte.class)
      .put(ShortObjectInspector.class, Short.class)
      .put(HiveCharObjectInspector.class, Character.class)
      .put(DateObjectInspector.class, Date.class)
      .put(TimestampObjectInspector.class, Timestamp.class)
      .put(HiveDecimalObjectInspector.class, BigDecimal.class)
      .build();

  private final ListObjectInspector listOI;
  private Object values;

  public HiveTable(Type type, ListObjectInspector listOI) {
    super(type);
    this.listOI = listOI;
  }

  public HiveTable updateValues(Object values) {
    this.values = values;
    return this;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      public Enumerator<T> enumerator() {
        if (values == null) {
          return Linq4j.<T>emptyEnumerator();
        } else {
          return (Enumerator<T>) new HiveEnumerator(values, listOI);
        }
      }
    };
  }

  @Override
  public Statistic getStatistic() {
    if (values == null) {
      return Statistics.of(0.0, ImmutableList.<ImmutableBitSet>of());
    }
    return Statistics.of(listOI.getListLength(values), ImmutableList.<ImmutableBitSet>of());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // Force primitives to look like a structure for Optiq
    return getRelType(listOI.getListElementObjectInspector(), typeFactory, true);
  }

  private static RelDataType getRelType(ObjectInspector oi, RelDataTypeFactory typeFactory, boolean asStructure) {
    switch (oi.getCategory()) {
      case PRIMITIVE:
        for (Map.Entry<Class, Class> e : typeMap.entrySet()) {
          if (e.getKey().isAssignableFrom(oi.getClass())) {
            RelDataType rdt = typeFactory.createJavaType(e.getValue());
            if (asStructure) {
              return typeFactory.createStructType(ImmutableList.of(rdt), ImmutableList.of("C1"));
            } else {
              return rdt;
            }
          }
        }
        throw new UnsupportedOperationException("Unsupported primitive type = " + oi);
      case LIST:
        //TODO: make this work
        throw new UnsupportedOperationException("Optiq does not support list types");
      case MAP:
        MapObjectInspector mapOI = (MapObjectInspector) oi;
        return typeFactory.createMapType(
            getRelType(mapOI.getMapKeyObjectInspector(), typeFactory, false),
            getRelType(mapOI.getMapValueObjectInspector(), typeFactory, false));
      case STRUCT:
        StructObjectInspector soi = (StructObjectInspector) oi;
        List<RelDataType> types = Lists.newArrayList();
        List<String> names = Lists.newArrayList();
        for (StructField field : soi.getAllStructFieldRefs()) {
          types.add(getRelType(field.getFieldObjectInspector(), typeFactory, false));
          names.add(field.getFieldName().toUpperCase(Locale.ENGLISH));
        }
        return typeFactory.createStructType(types, names);
      case UNION:
        throw new UnsupportedOperationException("Optiq does not support union types");
    }
    throw new IllegalStateException("Unknown field type" + oi);
  }

  public boolean isEmpty() {
    return values == null || listOI.getListLength(values) == 0;
  }
}
