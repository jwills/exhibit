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

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Functor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.javascript.JSCalculator;
import com.cloudera.exhibit.javascript.JSFunctor;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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

  public static Exhibit getExhibit(ObjectInspector[] args) throws UDFArgumentException {
    Map<String, Frame> frames = Maps.newHashMap();
    Map<String, Vec> vectors = Maps.newHashMap();
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    List<PrimitiveObjectInspector> pois = Lists.newArrayList();
    for (int i = 1; i < args.length; i++) {
      ObjectInspector oi = args[i];
      if (oi.getCategory() == ObjectInspector.Category.LIST) {
        ListObjectInspector loi = (ListObjectInspector) oi;
        ObjectInspector inner = loi.getListElementObjectInspector();
        String label = "t" + i;
        if (inner.getCategory() == ObjectInspector.Category.STRUCT) {
          frames.put(label, new HiveFrame(loi));
        } else {
          vectors.put(label, new HiveVector(getFieldType(inner), loi));
        }
      } else if (oi.getCategory() == ObjectInspector.Category.STRUCT) {
        StructObjectInspector soi = (StructObjectInspector) oi;
        for (StructField sf : soi.getAllStructFieldRefs()) {
          if (sf.getFieldObjectInspector().getCategory() == ObjectInspector.Category.LIST) {
            ListObjectInspector loi = (ListObjectInspector) sf.getFieldObjectInspector();
            ObjectInspector inner = loi.getListElementObjectInspector();
            if (inner.getCategory() == ObjectInspector.Category.STRUCT) {
              frames.put(sf.getFieldName(), new HiveFrame(loi));
            } else {
              vectors.put(sf.getFieldName(), new HiveVector(getFieldType(inner), loi));
            }
          } else if (sf.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) sf.getFieldObjectInspector();
            FieldType ft = getFieldType(poi);
            if (ft != null) {
              fields.add(new ObsDescriptor.Field(sf.getFieldName(), ft));
              pois.add(poi);
            }
          }
        }
      }
    }
    ObsDescriptor attrDesc = new SimpleObsDescriptor(fields);
    return new SimpleExhibit(new HiveAttributes(attrDesc, pois), frames, vectors);
  }

  public static void update(Exhibit exhibit, ObjectInspector[] inspectors, GenericUDF.DeferredObject[] args)
      throws HiveException {
    for (int i = 1; i < args.length; i++) {
      if (inspectors[i].getCategory() == ObjectInspector.Category.LIST) {
        update(exhibit, "t" + i, args[i].get());
      } else if (inspectors[i].getCategory() == ObjectInspector.Category.STRUCT) {
        StructObjectInspector soi = (StructObjectInspector) inspectors[i];
        Object base = args[i].get();
        for (StructField sf : soi.getAllStructFieldRefs()) {
          update(exhibit, sf.getFieldName(), soi.getStructFieldData(base, sf));
        }
      }
    }
  }

  public static void update(Exhibit exhibit, ObjectInspector[] inspectors, Object[] args) {
    for (int i = 1; i < args.length; i++) {
      if (inspectors[i] instanceof ListObjectInspector) {
        update(exhibit, "t" + i, args[i]);
      } else if (inspectors[i] instanceof StructObjectInspector) {
        StructObjectInspector soi = (StructObjectInspector) inspectors[i];
        Object base = args[i];
        for (StructField sf : soi.getAllStructFieldRefs()) {
          update(exhibit, sf.getFieldName(), soi.getStructFieldData(base, sf));
        }
      }
    }
  }

  public static void update(Exhibit exhibit, String label, Object newValues) {
    if (exhibit.frames().containsKey(label)) {
      ((HiveFrame) exhibit.frames().get(label)).updateValues(newValues);
    } else if (exhibit.vectors().containsKey(label)) {
      ((HiveVector) exhibit.vectors().get(label)).updateValues(newValues);
    } else {
      int attrIndex = exhibit.attributes().descriptor().indexOf(label);
      if (attrIndex >= 0) {
        ((HiveAttributes) exhibit.attributes()).update(attrIndex, newValues);
      } else {
        throw new IllegalArgumentException("Unknown exhibit field name: " + label);
      }
    }
  }

  public static Calculator getCalculator(ObjectInspector first) throws UDFArgumentException {
    if (first instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) first;
      Object args = ObjectInspectorUtils.getWritableConstantValue(first);
      String engine = loi.getListElement(args, 0).toString();
      if ("js".equalsIgnoreCase(engine) || "javascript".equalsIgnoreCase(engine)) {
        return new JSCalculator(loi.getListElement(args, 1).toString());
      }
    }
    String[] queries = getCode(first);
    return new SQLCalculator(queries);
  }

  private static String[] getCode(ObjectInspector first) throws UDFArgumentException {
    String[] code = null;
    Object wcv = ObjectInspectorUtils.getWritableConstantValue(first);
    if (first instanceof StringObjectInspector) {
      code = new String[] { wcv.toString() };
    } else if (first instanceof ListObjectInspector) {
      ListObjectInspector lOI = (ListObjectInspector) first;
      int len = lOI.getListLength(wcv);
      if (lOI.getListElementObjectInspector() instanceof StringObjectInspector) {
        StringObjectInspector strOI = (StringObjectInspector) lOI.getListElementObjectInspector();
        code = new String[len];
        for (int i = 0; i < len; i++) {
          code[i] = strOI.getPrimitiveJavaObject(lOI.getListElement(wcv, i));
        }
      } else {
        throw new UDFArgumentException("Array of queries must be all strings");
      }
    }
    return code;
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
    return null;
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

  public static StructObjectInspector fromDescriptor(ExhibitDescriptor descriptor) {
    List<String> names = Lists.newArrayList();
    List<ObjectInspector> inspectors = Lists.newArrayList();
    for (ObsDescriptor.Field f : descriptor.attributes()) {
      names.add(f.name);
      inspectors.add(FIELD_TYPES_TO_OI.get(f.type));
    }
    for (Map.Entry<String, FieldType> ve : descriptor.vectors().entrySet()) {
      names.add(ve.getKey());
      inspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(FIELD_TYPES_TO_OI.get(ve.getValue())));
    }
    for (Map.Entry<String, ObsDescriptor> fe : descriptor.frames().entrySet()) {
      names.add(fe.getKey());
      inspectors.add(fromDescriptor(fe.getValue(), true));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
  }

  public static Functor getFunctor(ObjectInspector first) {
    if (first.getCategory() != ObjectInspector.Category.LIST) {
      throw new IllegalArgumentException("Functors must have a code type: js, m, etc.");
    }
    ListObjectInspector loi = (ListObjectInspector) first;
    Object args = ObjectInspectorUtils.getWritableConstantValue(first);
    String engine = loi.getListElement(args, 0).toString();
    String code = loi.getListElement(args, 1).toString();
    if ("js".equalsIgnoreCase(engine) || "javascript".equalsIgnoreCase(engine)) {
      return new JSFunctor(code);
    }
    throw new IllegalArgumentException("Unknown engine type: " + engine);
  }
}
