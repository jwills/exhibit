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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.collect.Maps;

import java.util.Map;

public class UpdatableExhibit implements Exhibit {

  private final Exhibit base;
  private final Map<String, Frame> frames;
  private final Map<String, Vector> vectors;
  private UpdatableExhibitDescriptor descriptor;

  public UpdatableExhibit(Exhibit base) {
    this.base = base;
    this.frames = Maps.newHashMap();
    this.vectors = Maps.newHashMap();
    this.descriptor = new UpdatableExhibitDescriptor(base.descriptor());
  }

  public UpdatableExhibit add(String name, Vector vector) {
    this.vectors.put(name, vector);
    this.descriptor.addVector(name, vector.getType());
    return this;
  }
  public UpdatableExhibit add(String name, Frame frame) {
    this.frames.put(name, frame);
    this.descriptor.addFrame(name, frame.descriptor());
    return this;
  }

  public UpdatableExhibit addAllVectors(Map<String, Vector> vectors) {
    for (Map.Entry<String, Vector> e : vectors.entrySet()) {
      add(e.getKey(), e.getValue());
    }
    return this;
  }

  public UpdatableExhibit addAllFrames(Map<String, Frame> frames) {
    for (Map.Entry<String, Frame> e : frames.entrySet()) {
      add(e.getKey(), e.getValue());
    }
    return this;
  }

  @Override
  public UpdatableExhibitDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public Obs attributes() {
    return base.attributes();
  }

  @Override
  public Map<String, Frame> frames() {
    Map<String, Frame> union = Maps.newHashMap();
    union.putAll(base.frames());
    union.putAll(frames);
    return union;
  }

  @Override
  public Map<String, Vector> vectors() {
    Map<String, Vector> union = Maps.newHashMap();
    union.putAll(base.vectors());
    union.putAll(vectors);
    return union;
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
