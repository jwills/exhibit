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
package com.cloudera.exhibit.udtf;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

public final class TypeUtils {

  private static final Map<Integer, Class> SQL_TYPES_TO_JAVA = ImmutableMap.<Integer, Class>builder()
      .put(Types.BIGINT, Long.class)
      .put(Types.BIT, Boolean.class)
      .put(Types.BOOLEAN, Boolean.class)
      .put(Types.CHAR, String.class)
      .put(Types.DATE, Date.class)
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

  private static final Map<Integer, ObjectInspector> SQL_TYPES_TO_OI = ImmutableMap.<Integer, ObjectInspector>builder()
      .put(Types.BIGINT, PrimitiveObjectInspectorFactory.javaLongObjectInspector)
      .put(Types.BIT, PrimitiveObjectInspectorFactory.javaBooleanObjectInspector)
      .put(Types.BOOLEAN, PrimitiveObjectInspectorFactory.javaBooleanObjectInspector)
      .put(Types.CHAR, PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      .put(Types.DATE, PrimitiveObjectInspectorFactory.javaDateObjectInspector)
      .put(Types.DOUBLE, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)
      .put(Types.FLOAT, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector) // Note: Yes, this is right.
      .put(Types.INTEGER, PrimitiveObjectInspectorFactory.javaIntObjectInspector)
      .put(Types.LONGVARCHAR, PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      .put(Types.REAL, PrimitiveObjectInspectorFactory.javaFloatObjectInspector)
      .put(Types.SMALLINT, PrimitiveObjectInspectorFactory.javaShortObjectInspector)
      .put(Types.TIMESTAMP, PrimitiveObjectInspectorFactory.javaTimestampObjectInspector)
      .put(Types.TINYINT, PrimitiveObjectInspectorFactory.javaShortObjectInspector)
      .put(Types.VARCHAR, PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      .build();

  public static Class getJavaClassForSQLType(int sqlType) {
    return SQL_TYPES_TO_JAVA.get(sqlType);
  }

  public static ObjectInspector getObjectInspectorForSQLType(int sqlType) {
    return SQL_TYPES_TO_OI.get(sqlType);
  }

  private TypeUtils() {}
}
