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
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Exhibits {

  private static final Map<FieldType, Object> DV = ImmutableMap.<FieldType, Object>builder()
      .put(FieldType.BOOLEAN, Boolean.FALSE)
      .put(FieldType.DATE, new java.sql.Date(System.currentTimeMillis()))
      .put(FieldType.DECIMAL, new BigDecimal(0))
      .put(FieldType.DOUBLE, Double.valueOf(0.0))
      .put(FieldType.FLOAT, Float.valueOf(0.0f))
      .put(FieldType.INTEGER, Integer.valueOf(0))
      .put(FieldType.LONG, Long.valueOf(0L))
      .put(FieldType.SHORT, Short.valueOf((short) 0))
      .put(FieldType.STRING, "")
      .put(FieldType.TIMESTAMP, new java.sql.Time(System.currentTimeMillis()))
      .build();

  public static Exhibit defaultValues(ExhibitDescriptor descriptor) {
    return defaultValues(descriptor, DV);
  }

  public static Set<String> nameSet(ExhibitDescriptor descriptor) {
    Set<String> names = Sets.newHashSet(Iterables.transform(descriptor.attributes(), new Function<ObsDescriptor.Field, String>() {
      @Override
      public String apply(ObsDescriptor.Field field) {
        return field.name;
      }
    }));
    names.addAll(descriptor.frames().keySet());
    names.addAll(descriptor.vectors().keySet());
    return names;
  }

  public static Exhibit defaultValues(
      ExhibitDescriptor descriptor, FieldType ft, Object value, Object... args) {
    Map<FieldType, Object> defaults = Maps.newHashMap(DV);
    defaults.put(ft, value);
    for (int i = 0; i < args.length; i += 2) {
      defaults.put((FieldType) args[i], args[i + 1]);
    }
    return defaultValues(descriptor, defaults);
  }

  public static Exhibit defaultValues(ExhibitDescriptor descriptor, Map<FieldType, Object> defaults) {
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
    Map<String, Vec> vectors = Maps.newHashMap();
    for (Map.Entry<String, FieldType> e : descriptor.vectors().entrySet()) {
      FieldType type = e.getValue();
      Vector vector = VectorBuilder.build(type, ImmutableList.of(defaults.get(type)));
      vectors.put(e.getKey(), vector);
    }
    return new SimpleExhibit(attrs, frames, vectors);
  }
}
