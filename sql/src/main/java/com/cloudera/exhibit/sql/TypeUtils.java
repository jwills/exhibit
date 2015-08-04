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
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

public class TypeUtils {

  private static final Map<Integer, Class> SQL_TYPES_TO_JAVA = ImmutableMap.<Integer, Class>builder()
      .put(Types.BIGINT, Long.class)
      .put(Types.BIT, Boolean.class)
      .put(Types.BOOLEAN, Boolean.class)
      .put(Types.CHAR, String.class)
      .put(Types.DATE, Date.class)
      .put(Types.DECIMAL, BigDecimal.class)
      .put(Types.DOUBLE, Double.class)
      .put(Types.FLOAT, Double.class) // Note: Yes, this is right.
      .put(Types.INTEGER, Integer.class)
      .put(Types.LONGVARCHAR, String.class)
      .put(Types.REAL, Float.class)
      .put(Types.SMALLINT, Short.class)
      .put(Types.TIME, Time.class)
      .put(Types.TIMESTAMP, Timestamp.class)
      .put(Types.TINYINT, Short.class)
      .put(Types.VARCHAR, String.class)
      .build();

  private static final Map<Integer, FieldType> SQL_TYPES_TO_FIELD_TYPES =
      ImmutableMap.<Integer, FieldType>builder()
              .put(Types.BIGINT, FieldType.LONG)
              .put(Types.BIT, FieldType.BOOLEAN)
              .put(Types.BOOLEAN, FieldType.BOOLEAN)
              .put(Types.CHAR, FieldType.STRING)
              .put(Types.DATE, FieldType.DATE)
              .put(Types.DECIMAL, FieldType.DECIMAL)
              .put(Types.DOUBLE, FieldType.DOUBLE)
              .put(Types.FLOAT, FieldType.DOUBLE) // Note: Yes, this is right.
              .put(Types.INTEGER, FieldType.INTEGER)
              .put(Types.LONGVARCHAR, FieldType.STRING)
              .put(Types.REAL, FieldType.FLOAT)
              .put(Types.SMALLINT, FieldType.SHORT)
              .put(Types.TIME, FieldType.TIME)
              .put(Types.TIMESTAMP, FieldType.TIMESTAMP)
              .put(Types.TINYINT, FieldType.SHORT)
              .put(Types.VARCHAR, FieldType.STRING)
              .build();

  protected static Map<FieldType, SqlTypeName> FIELD_TYPES_TO_SQL_TYPES = ImmutableMap.<FieldType, SqlTypeName>builder()
      .put(FieldType.DATE, SqlTypeName.DATE)
      .put(FieldType.TIMESTAMP, SqlTypeName.TIMESTAMP)
      .put(FieldType.BOOLEAN, SqlTypeName.BOOLEAN)
      .put(FieldType.DOUBLE, SqlTypeName.DOUBLE)
      .put(FieldType.FLOAT, SqlTypeName.FLOAT)
      .put(FieldType.INTEGER, SqlTypeName.INTEGER)
      .put(FieldType.LONG, SqlTypeName.BIGINT)
      .build();

  protected static Map<FieldType, Class> FIELD_TYPES_TO_JAVA_TYPES = ImmutableMap.<FieldType, Class>builder()
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

  public static Class getJavaClassForSQLType(int sqlType) {
    Class clazz = SQL_TYPES_TO_JAVA.get(sqlType);
    if (clazz == null) {
      throw new IllegalArgumentException("Unsupported sql type = " + sqlType);
    }
    return clazz;
  }

  public static FieldType getFieldTypeForSQLType(int sqlType) {
    FieldType ft = SQL_TYPES_TO_FIELD_TYPES.get(sqlType);
    if (ft == null) {
      throw new IllegalArgumentException("Unsupported sql type = " + sqlType);
    }
    return ft;
  }
}
