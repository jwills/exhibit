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
package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public final class HiveUtils {

  private static final Map<FieldType, ObjectInspector> FIELD_TYPES_TO_OI =
      ImmutableMap.<FieldType, ObjectInspector>builder()
      .put(FieldType.LONG, PrimitiveObjectInspectorFactory.javaLongObjectInspector)
      .put(FieldType.BOOLEAN, PrimitiveObjectInspectorFactory.javaBooleanObjectInspector)
      .put(FieldType.DATE, PrimitiveObjectInspectorFactory.javaDateObjectInspector)
      .put(FieldType.DECIMAL, PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector)
      .put(FieldType.DOUBLE, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)
      .put(FieldType.FLOAT, PrimitiveObjectInspectorFactory.javaFloatObjectInspector)
      .put(FieldType.INTEGER, PrimitiveObjectInspectorFactory.javaIntObjectInspector)
      .put(FieldType.SHORT, PrimitiveObjectInspectorFactory.javaShortObjectInspector)
      .put(FieldType.TIMESTAMP, PrimitiveObjectInspectorFactory.javaTimestampObjectInspector)
      .put(FieldType.STRING, PrimitiveObjectInspectorFactory.javaStringObjectInspector)
      .build();

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

  private static final Map<Class, FieldType> FIELD_TYPES = ImmutableMap.<Class, FieldType>builder()
          .put(Boolean.class, FieldType.BOOLEAN)
          .put(Integer.class, FieldType.INTEGER)
          .put(Long.class, FieldType.LONG)
          .put(Float.class, FieldType.FLOAT)
          .put(Double.class, FieldType.DOUBLE)
          .put(String.class, FieldType.STRING)
          .put(HiveDecimal.class, FieldType.DECIMAL)
          .put(BigDecimal.class, FieldType.DECIMAL)
          .put(Timestamp.class, FieldType.TIMESTAMP)
          .put(Short.class, FieldType.SHORT)
          .put(Date.class, FieldType.DATE)
          .build();

  public static FieldType getFieldType(ObjectInspector oi) {
    if (oi instanceof PrimitiveObjectInspector) {
      FieldType ft = FIELD_TYPES.get(((PrimitiveObjectInspector) oi).getJavaPrimitiveClass());
      if (ft != null) {
        return ft;
      }
    }
    throw new IllegalArgumentException("Unsupported object inspector type = " + oi);
  }

  public static HiveFrame getHiveFrame(ObjectInspector oi) throws UDFArgumentException {
    if (oi.getCategory() == ObjectInspector.Category.LIST) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      return new HiveFrame(loi);
    } else {
      throw new UDFArgumentException("Only arrays of structs/primitives are supported at this time");
    }
  }

  public static Object asJavaType(Object v) {
    if (v instanceof HiveDecimal) {
      return ((HiveDecimal) v).bigDecimalValue();
    } else if (v instanceof HiveVarchar) {
      return ((HiveVarchar) v).getValue();
    }
    return v;
  }

  public static Object asHiveType(Object v) {
    if (v instanceof BigDecimal) {
      return HiveDecimal.create((BigDecimal) v);
    }
    return v;
  }

  private HiveUtils() {}

  public static ObjectInspector fromDescriptor(ObsDescriptor descriptor, boolean asStruct) {
    if (asStruct || descriptor.size() > 1) {
      List<String> names = Lists.newArrayList();
      List<ObjectInspector> inspectors = Lists.newArrayList();
      for (ObsDescriptor.Field f : descriptor) {
        names.add(f.name);
        inspectors.add(FIELD_TYPES_TO_OI.get(f.type));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
    } else {
      return FIELD_TYPES_TO_OI.get(descriptor.get(0).type);
    }
  }
}
