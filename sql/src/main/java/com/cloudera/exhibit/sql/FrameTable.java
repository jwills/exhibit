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
package com.cloudera.exhibit.sql;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class FrameTable extends AbstractTable implements QueryableTable {

  private final ObsDescriptor descriptor;
  private Frame frame;

  public FrameTable(ObsDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  public FrameTable updateFrame(Frame frame) {
    this.frame = frame;
    return this;
  }

  @Override
  public Statistic getStatistic() {
    if (frame == null) {
      return Statistics.UNKNOWN;
    }
    return Statistics.of(frame.size(), ImmutableList.<ImmutableBitSet>of());
  }

  private static Map<FieldType, SqlTypeName> TYPE_NAMES = ImmutableMap.<FieldType, SqlTypeName>builder()
      .put(FieldType.DATE, SqlTypeName.DATE)
      .put(FieldType.TIMESTAMP, SqlTypeName.TIMESTAMP)
      .put(FieldType.BOOLEAN, SqlTypeName.BOOLEAN)
      .put(FieldType.DOUBLE, SqlTypeName.DOUBLE)
      .put(FieldType.FLOAT, SqlTypeName.FLOAT)
      .put(FieldType.INTEGER, SqlTypeName.INTEGER)
      .put(FieldType.LONG, SqlTypeName.BIGINT)
      .build();

  private static Map<FieldType, Class> TYPE_CLASSES = ImmutableMap.<FieldType, Class>builder()
      .put(FieldType.DATE, Date.class)
      .put(FieldType.TIMESTAMP, Timestamp.class)
      .put(FieldType.DECIMAL, BigDecimal.class)
      .put(FieldType.SHORT, Short.class)
      .put(FieldType.BOOLEAN, Boolean.class)
      .put(FieldType.DOUBLE, Double.class)
      .put(FieldType.FLOAT, Float.class)
      .put(FieldType.INTEGER, Integer.class)
      .put(FieldType.LONG, Long.class)
      .put(FieldType.STRING, String.class)
      .build();

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<String> names = Lists.newArrayListWithExpectedSize(descriptor.size());
    List<RelDataType> relTypes = Lists.newArrayListWithExpectedSize(descriptor.size());
    for (int i = 0; i < descriptor.size(); i++) {
      ObsDescriptor.Field f = descriptor.get(i);
      names.add(f.name.toUpperCase());
      if (TYPE_NAMES.containsKey(f.type)) {
        relTypes.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(TYPE_NAMES.get(f.type)), true));
      } else {
        relTypes.add(typeFactory.createJavaType(TYPE_CLASSES.get(f.type)));
      }
    }
    return typeFactory.createStructType(relTypes, names);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schemaPlus, this, tableName) {
      public Enumerator<T> enumerator() {
        if (frame == null) {
          return Linq4j.<T>emptyEnumerator();
        } else {
          return (Enumerator<T>) new FrameEnumerator(frame);
        }
      }
    };
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String tableName, Class clazz) {
    return Schemas.tableExpression(schemaPlus, getElementType(), tableName, clazz);
  }
}
