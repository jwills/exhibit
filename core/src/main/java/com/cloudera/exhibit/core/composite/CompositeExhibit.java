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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CompositeExhibit implements Exhibit {

  private CompositeExhibitDescriptor descriptor;
  private Obs attributes;
  private Map<String, Frame> frames;
  private Map<String, Vector> vectors;

  public CompositeExhibit(Iterable<Exhibit> components) {
    this.descriptor = new CompositeExhibitDescriptor(Iterables.transform(components, new Function<Exhibit, ExhibitDescriptor>() {
      @Override
      public ExhibitDescriptor apply(Exhibit e) {
        return e.descriptor();
      }
    }));
    frames = Maps.newHashMap();
    vectors = Maps.newHashMap();
    List<Obs> attrs = Lists.newArrayList();
    for (Exhibit e : components) {
      attrs.add(e.attributes());
      frames.putAll(e.frames());
      vectors.putAll(e.vectors());
    }
   this.attributes = new CompositeObs(this.descriptor.attributes(), attrs);
  }

  public static CompositeExhibit of(Exhibit... components) {
    return new CompositeExhibit(Arrays.asList(components));
  }

  CompositeExhibit(CompositeExhibitDescriptor descriptor, Obs attributes, Map<String, Frame> frames, Map<String, Vector> vectors) {
    this.descriptor = descriptor;
    this.attributes = attributes;
    this.frames = frames;
    this.vectors= vectors;
  }

  @Override
  public CompositeExhibitDescriptor descriptor() {
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
  public Map<String, Vector> vectors() {
    return vectors;
  }

  @Override
  public boolean equals(Object o) {
    if( this == o ) {
      return true;
    }
    if (o == null || !(o instanceof Exhibit )) {
      return false;
    }
    Exhibit other = (Exhibit) o;
    return attributes() == other.attributes()
        && frames().equals(other.frames())
        && vectors().equals(other.vectors());
  }
}
