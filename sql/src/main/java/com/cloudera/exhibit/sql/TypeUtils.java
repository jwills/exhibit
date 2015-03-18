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
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.sql.Date;
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
      .put(Types.TIMESTAMP, Timestamp.class)
      .put(Types.TINYINT, Short.class)
      .put(Types.VARCHAR, String.class)
      .build();

  private static final Map<Integer, ObsDescriptor.FieldType> SQL_TYPES_TO_FIELD_TYPES =
      ImmutableMap.<Integer, ObsDescriptor.FieldType>builder()
              .put(Types.BIGINT, ObsDescriptor.FieldType.LONG)
              .put(Types.BIT, ObsDescriptor.FieldType.BOOLEAN)
              .put(Types.BOOLEAN, ObsDescriptor.FieldType.BOOLEAN)
              .put(Types.CHAR, ObsDescriptor.FieldType.STRING)
              .put(Types.DATE, ObsDescriptor.FieldType.DATE)
              .put(Types.DECIMAL, ObsDescriptor.FieldType.DECIMAL)
              .put(Types.DOUBLE, ObsDescriptor.FieldType.DOUBLE)
              .put(Types.FLOAT, ObsDescriptor.FieldType.DOUBLE) // Note: Yes, this is right.
              .put(Types.INTEGER, ObsDescriptor.FieldType.INTEGER)
              .put(Types.LONGVARCHAR, ObsDescriptor.FieldType.STRING)
              .put(Types.REAL, ObsDescriptor.FieldType.FLOAT)
              .put(Types.SMALLINT, ObsDescriptor.FieldType.SHORT)
              .put(Types.TIMESTAMP, ObsDescriptor.FieldType.TIMESTAMP)
              .put(Types.TINYINT, ObsDescriptor.FieldType.SHORT)
              .put(Types.VARCHAR, ObsDescriptor.FieldType.STRING)
              .build();

  public static Class getJavaClassForSQLType(int sqlType) {
    return SQL_TYPES_TO_JAVA.get(sqlType);
  }

  public static ObsDescriptor.FieldType getFieldTypeForSQLType(int sqlType) { return SQL_TYPES_TO_FIELD_TYPES.get(sqlType); }
}
