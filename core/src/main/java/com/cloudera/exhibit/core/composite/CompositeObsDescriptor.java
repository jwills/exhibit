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
package com.cloudera.exhibit.core.composite;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompositeObsDescriptor implements ObsDescriptor {

  private List<ObsDescriptor> components;
  private Map<String, Integer> fieldNames;
  private int[] offsets;

  public CompositeObsDescriptor(List<ObsDescriptor> components) {
    this.components = components;
    this.fieldNames = Maps.newHashMap();
    this.offsets = new int[components.size() + 1];
    int idx = 0;
    for (int i = 1; i < offsets.length; i++) {
      ObsDescriptor descriptor = components.get(i - 1);
      offsets[i] = offsets[i - 1] + descriptor.size();
      for (Field f : descriptor) {
        if (fieldNames.containsKey(f.name)) {
          throw new IllegalStateException("Duplicate field name in composite obs: " + f.name);
        }
        fieldNames.put(f.name, idx);
        idx++;
      }
    }
  }

  public int getOffset(int index) {
    return Arrays.binarySearch(offsets, index);
  }

  @Override
  public Field get(int i) {
    int offsetIndex = getOffset(i);
    int compIdx = 0;
    if (offsetIndex < 0) {
      offsetIndex = 1 - offsetIndex;
      compIdx = i - offsetIndex;
    }
    return components.get(offsetIndex).get(compIdx);
  }

  @Override
  public int indexOf(String name) {
    Integer ret = fieldNames.get(name);
    return ret == null ? -1 : ret;
  }

  @Override
  public int size() {
    return offsets[offsets.length - 1];
  }

  @Override
  public Iterator<Field> iterator() {
    return Iterators.concat(Lists.transform(components, new Function<ObsDescriptor, Iterator<Field>>() {
      @Override
      public Iterator<Field> apply(ObsDescriptor descriptor) {
        return descriptor.iterator();
      }
    }).iterator());
  }
}
