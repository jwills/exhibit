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

import com.google.common.collect.ImmutableList;
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
import org.apache.thrift.TBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.BitSet;
import java.util.List;

public class ThriftTable extends AbstractQueryableTable implements TranslatableTable {

  private List<? extends TBase> records;
  private final ThriftHelper helper;

  public ThriftTable(Class<? extends TBase> thriftClass) {
    super(Object[].class);
    this.helper = new ThriftHelper(thriftClass);
  }

  public ThriftTable(List<? extends TBase> records) {
    super(Object[].class);
    this.records = records;
    this.helper = new ThriftHelper(records.get(0).getClass());
  }

  public ThriftTable updateValues(List<? extends TBase> records) {
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
          return (Enumerator<T>) new ThriftEnumerator(records, helper);
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
    return helper.getRowType(typeFactory);
  }
}
