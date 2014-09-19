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
package com.cloudera.exhibit.avro;

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
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AvroTable extends AbstractQueryableTable implements TranslatableTable {

  private final Schema schema;
  private List<? extends IndexedRecord> records;
  private final BitSet stringFields;

  public AvroTable(Schema schema) {
    super(Object[].class);
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD);
    this.schema = schema;
    this.stringFields = getStringFields(schema);
  }

  public AvroTable(List<? extends IndexedRecord> records) {
    super(Object[].class);
    this.schema = records.get(0).getSchema();
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD);
    this.records = records;
    this.stringFields = getStringFields(schema);
  }

  static BitSet getStringFields(Schema schema) {
    BitSet stringFields = new BitSet(schema.getFields().size());
    for (int i = 0; i < schema.getFields().size(); i++) {
      if (schema.getFields().get(i).schema().getType() == Schema.Type.STRING) {
        stringFields.set(i);
      }
    }
    return stringFields;
  }

  public AvroTable updateValues(List<? extends IndexedRecord> records) {
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
          return (Enumerator<T>) new AvroEnumerator(records, AvroTable.this.schema, stringFields);
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
    return getRelType(schema, typeFactory);
  }

  private static Map<Schema.Type, Class> TYPE_CLASSES = ImmutableMap.<Schema.Type, Class>builder()
      .put(Schema.Type.BOOLEAN, Boolean.class)
      .put(Schema.Type.DOUBLE, Double.class)
      .put(Schema.Type.FLOAT, Float.class)
      .put(Schema.Type.INT, Integer.class)
      .put(Schema.Type.LONG, Long.class)
      .put(Schema.Type.STRING, String.class)
      .build();

  private static RelDataType getRelType(Schema schema, RelDataTypeFactory typeFactory) {
    Schema.Type st = schema.getType();
    if (TYPE_CLASSES.containsKey(st)) {
      return typeFactory.createJavaType(TYPE_CLASSES.get(st));
    }
    switch (schema.getType()) {
      case RECORD:
        List<RelDataType> types = Lists.newArrayList();
        List<String> names = Lists.newArrayList();
        for (Schema.Field f : schema.getFields()) {
          names.add(f.name().toUpperCase(Locale.ENGLISH));
          types.add(getRelType(f.schema(), typeFactory));
        }
        return typeFactory.createStructType(types, names);
      case MAP:
        typeFactory.createMapType(typeFactory.createJavaType(String.class),
            getRelType(schema.getValueType(), typeFactory));
      case ARRAY:
        throw new UnsupportedOperationException("Optiq does not support list types");
      case UNION:
        int nullIndex = nullIndex(schema);
        if (nullIndex >= 0) {
          Schema nonNullType = schema.getTypes().get(1 - nullIndex);
          return typeFactory.createTypeWithNullability(getRelType(nonNullType, typeFactory), true);
        } else {
          throw new UnsupportedOperationException("Optiq does not support union types");
        }
      default:
        throw new UnsupportedOperationException("Unsupported Avro data type: " + st);
    }
  }

  private static int nullIndex(Schema schema) {
    if (schema.getTypes().size() == 2) {
      if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
        return 0;
      } else if (schema.getTypes().get(1).getType() == Schema.Type.NULL) {
        return 1;
      }
    }
    return -1;
  }
}
