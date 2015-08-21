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
package com.cloudera.exhibit.core.functors;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Functor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterFunctor implements Functor, Serializable {

  private final Set<String> keep;
  private transient ExhibitDescriptor descriptor;

  public FilterFunctor(Iterable<String> keep) {
    this.keep = Sets.newHashSet(keep);
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor descriptor) {
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    Map<String, FieldType> vectors = Maps.newHashMap();
    Map<String, ObsDescriptor> frames = Maps.newHashMap();

    for (Map.Entry<String, ObsDescriptor> e : descriptor.frames().entrySet()) {
      if (keep.contains(e.getKey())) {
        frames.put(e.getKey(), e.getValue());
      }
    }
    for (Map.Entry<String, FieldType> e : descriptor.vectors().entrySet()) {
      if (keep.contains(e.getKey())) {
        vectors.put(e.getKey(), e.getValue());
      }
    }
    for (ObsDescriptor.Field f : descriptor.attributes()) {
      if (keep.contains(f.name)) {
        fields.add(f);
      }
    }
    this.descriptor = new ExhibitDescriptor(new SimpleObsDescriptor(fields), frames, vectors);
    return descriptor;
  }

  @Override
  public void cleanup() {
    // No-op
  }

  @Override
  public Exhibit apply(Exhibit exhibit) {
    if (exhibit == null) {
      return null;
    }
    if (descriptor == null) {
      initialize(exhibit.descriptor());
    }
    List<Object> attrs = Lists.newArrayList();
    Map<String, Vec> vectors = Maps.newHashMap();
    Map<String, Frame> frames = Maps.newHashMap();
    for (int i = 0; i < descriptor.attributes().size(); i++) {
      attrs.add(exhibit.attributes().get(i));
    }
    for (String vectorName : descriptor.vectors().keySet()) {
      vectors.put(vectorName, exhibit.vectors().get(vectorName));
    }
    for (String frameName : descriptor.frames().keySet()) {
      frames.put(frameName, exhibit.frames().get(frameName));
    }
    return new SimpleExhibit(new SimpleObs(descriptor.attributes(), attrs), frames, vectors);
  }
}
