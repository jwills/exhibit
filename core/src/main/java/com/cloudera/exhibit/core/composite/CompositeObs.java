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
  public Object get(int index) {
    int offset = descriptor.getOffset(index);
    int cmpIdx = 0;
    if (offset < 0) {
      offset = 1 - offset;
      cmpIdx = index - offset;
    }
    return components.get(offset).get(cmpIdx);
  }
}
