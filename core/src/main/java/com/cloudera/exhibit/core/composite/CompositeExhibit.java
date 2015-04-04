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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class CompositeExhibit implements Exhibit {

  private ExhibitDescriptor descriptor;
  private Obs attributes;
  private Map<String, Frame> frames;

  public static CompositeExhibit create(List<Exhibit> components) {
    List<ObsDescriptor> descs = Lists.newArrayList();
    Map<String, ObsDescriptor> frameDescs = Maps.newHashMap();
    for (Exhibit e : components) {
      descs.add(e.descriptor().attributes());
      frameDescs.putAll(e.descriptor().frames());
    }
    return create(new ExhibitDescriptor(new CompositeObsDescriptor(descs), frameDescs), components);
  }

  public static CompositeExhibit create(ExhibitDescriptor descriptor, List<Exhibit> components) {
    Map<String, Frame> frames = Maps.newHashMap();
    List<Obs> attrs = Lists.newArrayList();
    for (Exhibit e : components) {
      attrs.add(e.attributes());
      frames.putAll(e.frames());
    }
    CompositeObsDescriptor cod = (CompositeObsDescriptor) descriptor.attributes();
    return new CompositeExhibit(descriptor, new CompositeObs(cod, attrs), frames);
  }

  CompositeExhibit(ExhibitDescriptor descriptor, Obs attributes, Map<String, Frame> frames) {
    this.descriptor = descriptor;
    this.attributes = attributes;
    this.frames = frames;
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
}
