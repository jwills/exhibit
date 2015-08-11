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
package com.cloudera.exhibit.core.simple;

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

public class SimpleExhibit implements Exhibit {

  private final Obs attributes;
  private final Map<String, Frame> frames;
  private final Map<String, Vector> vectors;

  public static SimpleExhibit of(String name, Vector vec, Object... args) {
    Map<String, Vector>   v = Maps.newHashMap();
    Map<String, Frame> m = Maps.newHashMap();
    v.put(name, vec);
    for (int i = 0; i < args.length; i += 2) {
      v.put(args[i].toString(), (Vector) args[i + 1]);
    }
    return new SimpleExhibit(Obs.EMPTY, m, v);
  }

  public static SimpleExhibit of(String name, Frame frame, Object... args) {
    Map<String, Vector>   v = Maps.newHashMap();
    Map<String, Frame> m = Maps.newHashMap();
    m.put(name, frame);
    for (int i = 0; i < args.length; i += 2) {
      m.put(args[i].toString(), (Frame) args[i + 1]);
    }
    return new SimpleExhibit(Obs.EMPTY, m, v);
  }

  public SimpleExhibit(Obs attributes, Map<String, Frame> frames) {
    this.attributes = attributes;
    this.frames = frames;
    this.vectors = Maps.newHashMap();
  }

  public SimpleExhibit(Obs attributes, Map<String, Frame> frames, Map<String, Vector> vectors) {
    this.attributes = attributes;
    this.frames = frames;
    this.vectors = vectors;
  }

  @Override
  public SimpleExhibitDescriptor descriptor() {
    return new SimpleExhibitDescriptor(attributes.descriptor(),
      Maps.transformValues(frames, new Function<Frame, ObsDescriptor>() {
        @Override
        public ObsDescriptor apply(Frame frame) {
          return frame.descriptor();
        }
      }),
      Maps.transformValues(vectors, new Function<Vector, FieldType>() {
        @Override
        public FieldType apply(Vector vector) {
          return vector.getType();
        }
      })
    );
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Obs:\n");
    sb.append(attributes);
    sb.append("\n");
    for (Map.Entry<String, Frame> e : frames.entrySet()) {
      sb.append("Frame ").append(e.getKey()).append(": [\n");
      for (Obs obs : e.getValue()) {
        sb.append(obs).append(",\n");
      }
      sb.append("]");
    }
    sb.append("\n");
    for (Map.Entry<String, Vector> e : vectors.entrySet()) {
      sb.append("Vector ").append(e.getKey()).append(": [\n");
      for (Object obs : e.getValue()) {
        sb.append(obs.toString()).append(",\n");
      }
      sb.append("]");
    }
    return sb.toString();
  }

  public static final SimpleExhibit EMPTY = new SimpleExhibit(Obs.EMPTY, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
}
