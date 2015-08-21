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

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Type;
import java.util.List;

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

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<String> names = Lists.newArrayListWithExpectedSize(descriptor.size());
    List<RelDataType> relTypes = Lists.newArrayListWithExpectedSize(descriptor.size());
    for (int i = 0; i < descriptor.size(); i++) {
      ObsDescriptor.Field f = descriptor.get(i);
      names.add(f.name.toUpperCase());
      if (TypeUtils.FIELD_TYPES_TO_SQL_TYPES.containsKey(f.type)) {
        relTypes.add(typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(TypeUtils.FIELD_TYPES_TO_SQL_TYPES.get(f.type)), true));
      } else {
        relTypes.add(typeFactory.createJavaType(TypeUtils.FIELD_TYPES_TO_JAVA_TYPES.get(f.type)));
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
