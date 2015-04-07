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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.PivotCalculator;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.javascript.JSCalculator;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MetricConfig implements Serializable {
  public Map<String, String> descriptor = Maps.newLinkedHashMap();

  public String engine = "sql";

  public String code = "";

  public List<PivotCalculator.Key> pivot = Lists.newArrayList();

  public Calculator getCalculator() {
    ObsDescriptor od = null;
    if (descriptor != null && !descriptor.isEmpty()) {
      List<ObsDescriptor.Field> fields = Lists.newArrayList();
      for (Map.Entry<String, String> e : descriptor.entrySet()) {
        fields.add(new ObsDescriptor.Field(e.getKey(),
                ObsDescriptor.FieldType.valueOf(e.getValue().toUpperCase(Locale.ENGLISH))));
      }
      od = new SimpleObsDescriptor(fields);
    }
    if ("sql".equalsIgnoreCase(engine)) {
      SQLCalculator sql = SQLCalculator.create(od, code);
      if (pivot.isEmpty()) {
        return sql;
      } else {
        return new PivotCalculator(sql, pivot);
      }
    } else if ("js".equalsIgnoreCase(engine) || "javascript".equalsIgnoreCase(engine)) {
      return new JSCalculator(od, code);
    } else {
      throw new IllegalStateException("Unknown engine type: " + engine);
    }
  }
}
