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
package com.cloudera.exhibit.core.calculators;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;

public class LookupCalculator implements Calculator {

  private final String frame;

  public LookupCalculator(String frame) {
    this.frame = frame;
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor descriptor) {
    return descriptor.frames().get(frame);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public Iterable<Obs> apply(Exhibit exhibit) {
    return exhibit.frames().get(frame);
  }
}
