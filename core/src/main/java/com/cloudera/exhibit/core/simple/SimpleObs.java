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
package com.cloudera.exhibit.core.simple;

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

public class SimpleObs extends Obs {

  private final ObsDescriptor descriptor;
  private final List<Object> values;

  public static SimpleObs of(ObsDescriptor desc, Object... args) {
    return new SimpleObs(desc, Lists.newArrayList(args));
  }

  public SimpleObs(ObsDescriptor descriptor, List<Object> values) {
    assert(descriptor.size() == values.size());
    this.descriptor = Preconditions.checkNotNull(descriptor);
    this.values = Preconditions.checkNotNull(values);
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public Object get(int index) {
    return values.get(index);
  }

  public List<Object> getValues() {
    return values;
  }

  @Override
  public int hashCode() {
    return descriptor.hashCode() + 17 * values.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof SimpleObs)) {
      return false;
    }
    SimpleObs obs = (SimpleObs) other;
    return descriptor.equals(obs.descriptor) && values.equals(obs.values);
  }
}
