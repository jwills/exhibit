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
package com.cloudera.exhibit.etl.config;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.calculators.PivotCalculator;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.javascript.JSCalculator;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FrameConfig implements Serializable {

  // An optional name for the generated frame (required for temp frames.)
  public String name = "";

  // The engine to use. Currently, only "sql" and "js"/"javascript" are supported.
  public String engine = "sql";

  // The actual code to execute against the exhibits using the given engine.
  public String code = "";

  // For scripting engines, we cannot accurately infer the return type of the code before a job starts.
  // Therefore, the user needs to manually specify the names/FieldTypes of the returned object/list so that
  // we can generate the output schemas for this computation. For SQL, this type inference can be done
  // automatically, so this field is optional.
  public Map<String, String> descriptor = Maps.newLinkedHashMap();

  // An optional pivot operation that should be applied to convert the result from a "long" to a "wide"
  // frame format.
  public PivotConfig pivot = null;

  public Calculator getCalculator() {
    ObsDescriptor od = null;
    if (descriptor != null && !descriptor.isEmpty()) {
      List<ObsDescriptor.Field> fields = Lists.newArrayList();
      for (Map.Entry<String, String> e : descriptor.entrySet()) {
        fields.add(new ObsDescriptor.Field(e.getKey(),
                FieldType.valueOf(e.getValue().toUpperCase(Locale.ENGLISH))));
      }
      od = new SimpleObsDescriptor(fields);
    }
    if ("sql".equalsIgnoreCase(engine)) {
      SQLCalculator sql = SQLCalculator.create(od, code);
      if (pivot == null) {
        return sql;
      } else {
        return new PivotCalculator(sql, pivot.by, toKeys(pivot.variables));
      }
    } else if ("js".equalsIgnoreCase(engine) || "javascript".equalsIgnoreCase(engine)) {
      return new JSCalculator(od, code);
    } else {
      throw new IllegalStateException("Unknown engine type: " + engine);
    }
  }

  static List<PivotCalculator.Key> toKeys(Map<String, List<String>> vars) {
    List<PivotCalculator.Key> keys = Lists.newArrayList();
    for (Map.Entry<String, List<String>> e : vars.entrySet()) {
      keys.add(new PivotCalculator.Key(e.getKey(), Sets.newHashSet(e.getValue())));
    }
    return keys;
  }

  public String toString() {
    return "Frame(" + code.substring(0, Math.min(code.length(), 25)) + ")";
  }
}
