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
import com.cloudera.exhibit.core.Functor;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class CompositeFunctor implements Functor {

  private List<Functor> functors;

  public static CompositeFunctor of(Functor... calcs) {
    return new CompositeFunctor(Arrays.asList(calcs));
  }

  public CompositeFunctor(List<Functor> functors) {
    this.functors = functors;
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor descriptor) {
    List<ExhibitDescriptor> ret = Lists.newArrayList();
    for (Functor c : functors) {
      ret.add(c.initialize(descriptor));
    }
    return new CompositeExhibitDescriptor(ret);
  }

  @Override
  public void cleanup() {
    for (Functor c : functors) {
      c.cleanup();
    }
  }

  @Override
  public Exhibit apply(Exhibit exhibit) {
    List<Exhibit> rets = Lists.newArrayList();
    for (Functor c : functors) {
      rets.add(c.apply(exhibit));
    }
    return new CompositeExhibit(rets);
  }
}
