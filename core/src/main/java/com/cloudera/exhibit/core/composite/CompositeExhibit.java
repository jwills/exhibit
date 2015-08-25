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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class CompositeExhibit implements Exhibit {

  private ExhibitDescriptor descriptor;
  private Obs attributes;
  private Map<String, Frame> frames;
  private Map<String, Vec> vectors;

  public static ExhibitDescriptor createDescriptor(Iterable<ExhibitDescriptor> descriptors) {
    List<ObsDescriptor> descs = Lists.newArrayList();
    Map<String, ObsDescriptor> frameDescs = Maps.newHashMap();
    Map<String, FieldType> vectorDescs = Maps.newHashMap();
    for (ExhibitDescriptor ed : descriptors) {
      descs.add(ed.attributes());
      frameDescs.putAll(ed.frames());
      vectorDescs.putAll(ed.vectors());
    }
    return new ExhibitDescriptor(new CompositeObsDescriptor(descs), frameDescs, vectorDescs);
  }

  public static CompositeExhibit create(Iterable<Exhibit> components) {
    return create(createDescriptor(Iterables.transform(components, new Function<Exhibit, ExhibitDescriptor>() {
      @Override
      public ExhibitDescriptor apply(Exhibit exhibit) {
        return exhibit.descriptor();
      }
    })), components);
  }

  public static CompositeExhibit create(ExhibitDescriptor descriptor, Iterable<Exhibit> components) {
    Map<String, Frame> frames = Maps.newHashMap();
    Map<String, Vec> vectors = Maps.newHashMap();
    List<Obs> attrs = Lists.newArrayList();
    for (Exhibit e : components) {
      attrs.add(e.attributes());
      frames.putAll(e.frames());
      vectors.putAll(e.vectors());
    }
    CompositeObsDescriptor cod = (CompositeObsDescriptor) descriptor.attributes();
    return new CompositeExhibit(descriptor, new CompositeObs(cod, attrs), frames, vectors);
  }

  CompositeExhibit(ExhibitDescriptor descriptor, Obs attributes, Map<String, Frame> frames, Map<String, Vec> vectors) {
    this.descriptor = descriptor;
    this.attributes = attributes;
    this.frames = frames;
    this.vectors = vectors;
  }

  @Override
  public ExhibitDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public Obs attributes() {
    return attributes;
  }

  @Override
  public Map<String, Frame> frames() {
    return frames;
  }

  @Override
  public Map<String, Vec> vectors() {
    return vectors;
  }
}
