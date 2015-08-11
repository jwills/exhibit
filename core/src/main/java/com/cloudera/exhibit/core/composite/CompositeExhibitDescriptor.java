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
package com.cloudera.exhibit.core.composite;

import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompositeExhibitDescriptor extends ExhibitDescriptor {
  private final CompositeObsDescriptor attributes;
  private final Map<String, ObsDescriptor> frames;
  private final Map<String, FieldType> vectors;

  public CompositeExhibitDescriptor(Iterable<ExhibitDescriptor> components){
    Set<String> attributeFields = Sets.newHashSet();
    List<ObsDescriptor> descs = Lists.newArrayList();
    Map<String, ObsDescriptor> frameDescs = Maps.newHashMap();
    Map<String, FieldType> vectorDescs = Maps.newHashMap();
    for (ExhibitDescriptor e : components) {
      for(ObsDescriptor.Field attr: e.attributes()) {
        if(attributeFields.contains(attr.name)) {
          throw new IllegalArgumentException("Duplicate attribute field: " + attr.name + " found in components");
        }
        attributeFields.add(attr.name);
      }
      Set<String> commonFrames = Sets.intersection(e.frames().keySet(), frameDescs.keySet());
      if(commonFrames.size() != 0){
        throw new IllegalArgumentException("Duplicate frame names found: " + commonFrames.toString() + " found in components");
      }
      Set<String> commonVectors = Sets.intersection(e.vectors().keySet(), vectorDescs.keySet());
      if(commonVectors.size() != 0){
        throw new IllegalArgumentException("Duplicate vector names found: " + commonVectors.toString() + " found in components");
      }
      descs.add(e.attributes());
      frameDescs.putAll(e.frames());
      vectorDescs.putAll(e.vectors());
    }
    attributes = new CompositeObsDescriptor(descs);
    vectors = vectorDescs;
    frames = frameDescs;
  }

  @Override
  public CompositeObsDescriptor attributes() {
    return attributes;
  }

  @Override
  public Map<String, ObsDescriptor> frames() {
    return frames;
  }

  @Override
  public Map<String, FieldType> vectors() {
    return vectors;
  }

  @Override
  public ExhibitDescriptor clone() {
    return null;
  }
}
