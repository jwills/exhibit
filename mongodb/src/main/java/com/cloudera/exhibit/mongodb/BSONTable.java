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
package com.cloudera.exhibit.mongodb;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
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
import org.bson.BSONObject;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class BSONTable extends AbstractQueryableTable implements TranslatableTable {

  private List<? extends BSONObject> records;
  private final List<String> names;
  private final List<String> columns;
  private final List<Object> defaultValues;

  public static BSONTable create(List<String> names, List<Object> defaultValues) {
    return create(names, defaultValues, ImmutableMap.<String, String>of());
  }

  public static BSONTable create(List<String> names, List<Object> defaultValues, Map<String, String> mappings) {
    return new BSONTable(names, defaultValues, mappings, null);
  }

  public static BSONTable create(List<? extends BSONObject> records) {
    return create(records, ImmutableMap.<String, String>of());
  }

  public static BSONTable create(List<? extends BSONObject> records, Map<String, String> mappings) {
    List<String> names = getNames(records.get(0));
    return new BSONTable(names, getTypes(names, records.get(0)), mappings, records);
  }

  BSONTable(List<String> names, List<Object> defaultValues, Map<String, String> mappings,
            List<? extends BSONObject> records) {
    super(Object[].class);
    this.names = Preconditions.checkNotNull(names);
    this.defaultValues = Preconditions.checkNotNull(defaultValues);
    this.columns = getColumns(names, mappings);
    Preconditions.checkArgument(!names.isEmpty(), "No column names specified");
    Preconditions.checkArgument(names.size() == defaultValues.size(), "names/defaultValues aren't equal");
    this.records = records;
  }

  private static List<String> getNames(BSONObject instance) {
    return ImmutableList.copyOf(instance.keySet());
  }

  private static List<Object> getTypes(List<String> names, BSONObject instance) {
    List<Object> types = Lists.newArrayListWithExpectedSize(names.size());
    for (int i = 0; i < names.size(); i++) {
      Object value = instance.get(names.get(i));
      if (value == null) {
        throw new IllegalArgumentException("No missing values are allowed during type inference");
      }
      types.add(value.getClass());
    }
    return types;
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

  public BSONTable updateValues(List<? extends BSONObject> records) {
    this.records = records;
    return this;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      public Enumerator<T> enumerator() {
        if (records == null) {
          return Linq4j.<T>emptyEnumerator();
        } else {
          return (Enumerator<T>) new BSONEnumerator(records, columns, defaultValues);
        }
      }
    };
  }

  @Override
  public Statistic getStatistic() {
    if (records == null) {
      return Statistics.UNKNOWN;
    }
    return Statistics.of(records.size(), ImmutableList.<BitSet>of());
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new JavaRules.EnumerableTableAccessRel(
        context.getCluster(),
        context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        relOptTable,
        (Class) getElementType());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> dataTypes = Lists.newArrayList();
    for (Object defaultValue : defaultValues) {
      if (defaultValue instanceof Class) {
        dataTypes.add(typeFactory.createJavaType((Class) defaultValue));
      } else {
        dataTypes.add(typeFactory.createJavaType(defaultValue.getClass()));
      }
    }
    return typeFactory.createStructType(dataTypes, Lists.transform(names, new Function<String, String>() {
      @Override
      public String apply(@Nullable String input) {
        return input.toUpperCase(Locale.ENGLISH);
      }
    }));
  }
}