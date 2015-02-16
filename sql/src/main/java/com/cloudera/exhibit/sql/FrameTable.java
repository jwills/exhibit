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
package com.cloudera.exhibit.sql;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class FrameTable extends AbstractQueryableTable implements TranslatableTable {

  private final ObsDescriptor descriptor;
  private Frame frame;

  public FrameTable(ObsDescriptor descriptor) {
    super(Object[].class);
    this.descriptor = descriptor;
  }

  public FrameTable(Frame frame) {
    this(frame.descriptor());
    this.frame = frame;
  }

  public FrameTable updateFrame(Frame frame) {
    assert(this.descriptor.equals(frame.descriptor()));
    this.frame = frame;
    return this;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
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
  public Statistic getStatistic() {
    if (frame == null) {
      return Statistics.UNKNOWN;
    }
    return Statistics.of(frame.size(), ImmutableList.<BitSet>of());
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new JavaRules.EnumerableTableAccessRel(
        context.getCluster(),
        context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        relOptTable,
        (Class) getElementType());
  }

  private static Map<FieldType, Class> TYPE_CLASSES = ImmutableMap.<FieldType, Class>builder()
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
      relTypes.add(typeFactory.createJavaType(TYPE_CLASSES.get(f.type)));
    }
    return typeFactory.createStructType(relTypes, names);
  }
}
