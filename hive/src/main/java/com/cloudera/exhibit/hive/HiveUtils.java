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
package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.OptiqHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.lang.reflect.Type;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public final class HiveUtils {

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

  public static ObjectInspector getObjectInspectorForSQLType(int sqlType) {
    return SQL_TYPES_TO_OI.get(sqlType);
  }

  public static String[] getQueries(ObjectInspector first) throws UDFArgumentException {
    String[] queries = null;
    if (ObjectInspectorUtils.isConstantObjectInspector(first)) {
      Object wcv = ObjectInspectorUtils.getWritableConstantValue(first);

      if (first instanceof StringObjectInspector) {
        queries = new String[] { wcv.toString() };
      } else if (first instanceof ListObjectInspector) {
        ListObjectInspector lOI = (ListObjectInspector) first;
        int len = lOI.getListLength(wcv);
        if (lOI.getListElementObjectInspector() instanceof StringObjectInspector) {
          StringObjectInspector strOI = (StringObjectInspector) lOI.getListElementObjectInspector();
          queries = new String[len];
          for (int i = 0; i < len; i++) {
            queries[i] = strOI.getPrimitiveJavaObject(lOI.getListElement(wcv, i));
          }
        } else {
          throw new UDFArgumentException("Array of queries must be all strings");
        }
      }
    } else {
      throw new UDFArgumentException("First argument must be a constant string or array of strings");
    }
    return queries;
  }

  public static HiveTable getHiveTable(ObjectInspector oi) throws UDFArgumentException {
    if (oi.getCategory() == ObjectInspector.Category.LIST) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      ObjectInspector elOI = loi.getListElementObjectInspector();
      if (elOI.getCategory() == ObjectInspector.Category.STRUCT ||
          elOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
        Type tableType = Object[].class;
        if (elOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
          tableType = ((PrimitiveObjectInspector) elOI).getJavaPrimitiveClass();
        }
        return new HiveTable(tableType, loi);
      } else {
        throw new UDFArgumentException("Only arrays of structs/primitives are supported at this time");
      }
    } else {
      throw new UDFArgumentException("Only arrays of structs/primitives are supported at this time");
    }
  }

  public static StructObjectInspector fromMetaData(OptiqHelper helper) throws UDFArgumentException, SQLException {
    Statement stmt = null;
    try {
      stmt = helper.newStatement();
      ResultSetMetaData metadata = helper.execute(stmt).getMetaData();
      int colCount = metadata.getColumnCount();
      if (colCount == 0) {
        throw new UDFArgumentException("No columns returned from query: " + helper.getLastQuery());
      }
      List<String> names = Lists.newArrayList();
      List<ObjectInspector> inspectors = Lists.newArrayList();
      for (int i = 1; i <= colCount; i++) {
        names.add(metadata.getColumnLabel(i));
        ObjectInspector oi = getObjectInspectorForSQLType(metadata.getColumnType(i));
        if (oi == null) {
          throw new UDFArgumentException("Unknown column type in result: " + metadata.getColumnTypeName(i));
        }
        inspectors.add(oi);
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
    } finally {
      helper.closeStatement(stmt);
    }
  }

  private HiveUtils() {}
}
