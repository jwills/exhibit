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

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;

import java.util.List;

public class CompositeObs extends Obs {

  private CompositeObsDescriptor descriptor;
  private List<Obs> components;

  public CompositeObs(List<Obs> components) {
    this.components = components;
  }

  public CompositeObs(CompositeObsDescriptor descriptor, List<Obs> components) {
    this.descriptor = descriptor;
    this.components = components;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    return descriptor().size();
  }

  @Override
  public Object get(int index) {
    int offsetIndex = descriptor.getOffsetIndex(index);
    int cmpIdx = index - descriptor.getOffset(offsetIndex);
    return components.get(offsetIndex).get(cmpIdx);
  }
}
