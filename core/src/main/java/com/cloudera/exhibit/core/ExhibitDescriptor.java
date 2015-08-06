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

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class ExhibitDescriptor implements Serializable {

  private final ObsDescriptor attributes;
  private final Map<String, ObsDescriptor> frames;
  private final Map<String, FieldType> vectors;

  public static ExhibitDescriptor of(String name, ObsDescriptor frame) {
    return new ExhibitDescriptor(ObsDescriptor.EMPTY
        , ImmutableMap.of(name, frame)
        , Collections.<String, FieldType>emptyMap());
  }

  public ExhibitDescriptor(ObsDescriptor attributes, Map<String, ObsDescriptor> frames, Map<String, FieldType> vectors) {
    this.attributes = attributes;
    this.frames = frames;
    this.vectors = vectors;
  }

  public ObsDescriptor attributes() {
    return attributes;
  }

  public Map<String, ObsDescriptor> frames() {
    return frames;
  }

  public Map<String, FieldType> vectors() {
    return vectors;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Attributes:\n");
    toStringHelper(sb, 2, attributes);
    sb.append("Frames:\n");
    for (Map.Entry<String, ObsDescriptor> e : frames.entrySet()) {
      sb.append("  ").append(e.getKey()).append("\n");
      toStringHelper(sb, 4, e.getValue());
    }
    sb.append("Vectors:\n");
    for (Map.Entry<String, FieldType> e : vectors.entrySet()) {
      sb.append("  ").append(e.getKey()).append("\n");
      toStringHelper(sb, 4, e.getKey(), e.getValue());
    }
    return sb.toString();
  }

  private static void toStringHelper(StringBuilder sb, int indent, String name, FieldType type) {
    for (int j = 0; j < indent; j++) {
      sb.append(' ');
    }
    sb.append(name).append(": ").append(type).append("\n");
  }

  private static void toStringHelper(StringBuilder sb, int indent, ObsDescriptor desc) {
    for (int i = 0; i < desc.size(); i++) {
      ObsDescriptor.Field f = desc.get(i);
      toStringHelper(sb, indent, f.name, f.type);
    }
  }
}
