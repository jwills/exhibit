/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import java.util.Map;

public class ExhibitDescriptor {

  private final ObsDescriptor attributes;
  private final Map<String, ObsDescriptor> frames;

  public static ExhibitDescriptor of(String name, ObsDescriptor frame) {
    return new ExhibitDescriptor(ObsDescriptor.EMPTY, ImmutableMap.of(name, frame));
  }

  public ExhibitDescriptor(ObsDescriptor attributes, Map<String, ObsDescriptor> frames) {
    this.attributes = attributes;
    this.frames = frames;
  }

  public ObsDescriptor attributes() {
    return attributes;
  }

  public Map<String, ObsDescriptor> frames() {
    return frames;
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
    return sb.toString();
  }

  private static void toStringHelper(StringBuilder sb, int indent, ObsDescriptor desc) {
    for (int i = 0; i < desc.size(); i++) {
      ObsDescriptor.Field f = desc.get(i);
      for (int j = 0; j < indent; j++) {
        sb.append(' ');
      }
      sb.append(f.name).append(": ").append(f.type).append("\n");
    }
  }
}
