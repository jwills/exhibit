/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.core;

import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PivotCalculator implements Calculator {

  public static class Key implements Serializable {
    String name;
    Set<String> levels;

    public Key(String name, Set<String> levels) {
      this.name = name;
      this.levels = levels;
    }
  }

  private Calculator fc;
  private Map<String, Set<String>> keys;
  private transient ObsDescriptor descriptor;

  public PivotCalculator(Calculator base, String key, Set<String> levels) {
    this(base, ImmutableList.of(new Key(key, levels)));
  }

  public PivotCalculator(Calculator base, List<Key> keys) {
    this.fc = base;
    this.keys = Maps.newLinkedHashMap();
    for (Key key : keys) {
      this.keys.put(key.name, key.levels);
    }
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor descriptor) {
    ObsDescriptor fd = fc.initialize(descriptor);
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    List<Set<String>> levelSet = Lists.newArrayList(keys.values());
    for (ObsDescriptor.Field f : fd) {
      if (!keys.containsKey(f.name)) {
        for (List<String> suffix : Sets.cartesianProduct(levelSet)) {
          StringBuilder sb = new StringBuilder(f.name);
          sb.append('_');
          sb.append(Joiner.on('_').join(suffix));
          fields.add(new ObsDescriptor.Field(sb.toString(), f.type));
        }
      }
    }
    return new SimpleObsDescriptor(fields);
  }

  @Override
  public void cleanup() {
    fc.cleanup();
  }

  @Override
  public Iterable<Obs> apply(Exhibit exhibit) {
    Iterable<Obs> frame = fc.apply(exhibit);
    if (descriptor == null) {
      descriptor = initialize(exhibit == null ? null : exhibit.descriptor());
    }
    List<Object> values = Arrays.asList(new Object[descriptor.size()]);
    for (Obs obs : frame) {
      List<String> keyValues = Lists.newArrayListWithExpectedSize(keys.size());
      for (String key : keys.keySet()) {
        Object v = obs.get(key);
        keyValues.add(v == null ? "null" : v.toString());
      }
      for (ObsDescriptor.Field f : obs.descriptor()) {
        if (!keys.containsKey(f.name)) {
          String retField = new StringBuilder(f.name).append('_').append(Joiner.on('_').join(keyValues)).toString();
          int index = descriptor.indexOf(retField);
          values.set(index, obs.get(f.name));
        }
      }
    }
    return ImmutableList.<Obs>of(new SimpleObs(descriptor, values));
  }
}
