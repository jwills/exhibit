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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

public class ResultSetTable extends AbstractTable implements QueryableTable {

  private final Class elementType;
  private final List<Object> values;
  private final RelProtoDataType protoDataType;

  public static ResultSetTable create(ResultSet rs) throws SQLException {
    int cols = rs.getMetaData().getColumnCount();
    List values = Lists.newArrayList();
    Class elementType = Object[].class;
    if (cols > 1) {
      while (rs.next()) {
        Object[] v = new Object[cols];
        for (int i = 0; i < cols; i++) {
          v[i] = rs.getObject(i + 1);
        }
        values.add(v);
      }
    } else {
      elementType = TypeUtils.getJavaClassForSQLType(rs.getMetaData().getColumnType(1));
      while (rs.next()) {
        values.add(rs.getObject(1));
      }
    }
    return new ResultSetTable(elementType, values, fromMetadata(rs.getMetaData()));
  }

  public static RelProtoDataType fromMetadata(ResultSetMetaData metadata) throws SQLException {
    int cols = metadata.getColumnCount();
    List<String> names = Lists.newArrayListWithExpectedSize(cols);
    List<Class> javaTypes = Lists.newArrayListWithExpectedSize(cols);
    for (int i = 1; i <= cols; i++) {
      names.add(metadata.getColumnLabel(i));
      javaTypes.add(TypeUtils.getJavaClassForSQLType(metadata.getColumnType(i)));
    }
    return new SQLTypeProtoDataType(names, javaTypes);
  }

  public ResultSetTable(Class elementType, List values, RelProtoDataType protoDataType) {
    this.elementType = elementType;
    this.values = values;
    this.protoDataType = protoDataType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return protoDataType.apply(relDataTypeFactory);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schemaPlus, this, tableName) {
      @Override
      public Enumerator<T> enumerator() {
        return (Enumerator<T>) Linq4j.enumerator(values);
      }
    };
  }

  @Override
  public Type getElementType() {
    return elementType;
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String tableName, Class clazz) {
    return Schemas.tableExpression(schemaPlus, getElementType(), tableName, clazz);
  }

  private static class SQLTypeProtoDataType implements RelProtoDataType {
    private final List<String> names;
    private final List<Class> javaTypes;

    public SQLTypeProtoDataType(List<String> names, List<Class> javaTypes) {
      Preconditions.checkArgument(!names.isEmpty(), "No columns in result table");
      Preconditions.checkArgument(names.size() == javaTypes.size(), "Different number of column names and types");
      this.names = names;
      this.javaTypes = javaTypes;
    }

    @Override
    public RelDataType apply(RelDataTypeFactory typeFactory) {
      List<RelDataType> dataTypes = Lists.newArrayList();
      for (Class clazz : javaTypes) {
        dataTypes.add(typeFactory.createJavaType(clazz));
      }
      return typeFactory.createStructType(dataTypes, Lists.transform(names, new Function<String, String>() {
        @Override
        public String apply(String input) {
          return input.toUpperCase(Locale.ENGLISH);
        }
      }));
    }
  }
}
