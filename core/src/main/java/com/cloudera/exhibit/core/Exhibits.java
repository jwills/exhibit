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
package com.cloudera.exhibit.core;

import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class Exhibits {

  private static final Map<ObsDescriptor.FieldType, Object> DV = ImmutableMap.<ObsDescriptor.FieldType, Object>builder()
      .put(ObsDescriptor.FieldType.BOOLEAN, Boolean.FALSE)
      .put(ObsDescriptor.FieldType.DATE, new java.sql.Date(System.currentTimeMillis()))
      .put(ObsDescriptor.FieldType.DECIMAL, new BigDecimal(0))
      .put(ObsDescriptor.FieldType.DOUBLE, Double.valueOf(0.0))
      .put(ObsDescriptor.FieldType.FLOAT, Float.valueOf(0.0f))
      .put(ObsDescriptor.FieldType.INTEGER, Integer.valueOf(0))
      .put(ObsDescriptor.FieldType.LONG, Long.valueOf(0L))
      .put(ObsDescriptor.FieldType.SHORT, Short.valueOf((short) 0))
      .put(ObsDescriptor.FieldType.STRING, "")
      .put(ObsDescriptor.FieldType.TIMESTAMP, new java.sql.Time(System.currentTimeMillis()))
      .build();

  public static Exhibit defaultValues(ExhibitDescriptor descriptor) {
    return defaultValues(descriptor, DV);
  }

  public static Exhibit defaultValues(
      ExhibitDescriptor descriptor, ObsDescriptor.FieldType ft, Object value, Object... args) {
    Map<ObsDescriptor.FieldType, Object> defaults = Maps.newHashMap(DV);
    defaults.put(ft, value);
    for (int i = 0; i < args.length; i += 2) {
      defaults.put((ObsDescriptor.FieldType) args[i], args[i + 1]);
    }
    return defaultValues(descriptor, defaults);
  }

  public static Exhibit defaultValues(ExhibitDescriptor descriptor, Map<ObsDescriptor.FieldType, Object> defaults) {
    List<Object> attrValues = Lists.newArrayList();
    for (ObsDescriptor.Field f : descriptor.attributes()) {
      attrValues.add(defaults.get(f.type));
    }
    Obs attrs = new SimpleObs(descriptor.attributes(), attrValues);
    Map<String, Frame> frames = Maps.newHashMap();
    for (Map.Entry<String, ObsDescriptor> e : descriptor.frames().entrySet()) {
      List<Object> frameValues = Lists.newArrayList();
      for (ObsDescriptor.Field f : e.getValue()) {
        frameValues.add(defaults.get(f.type));
      }
      Obs frameObs = new SimpleObs(e.getValue(), frameValues);
      frames.put(e.getKey(), new SimpleFrame(ImmutableList.of(frameObs)));
    }
    return new SimpleExhibit(attrs, frames);
  }
}
