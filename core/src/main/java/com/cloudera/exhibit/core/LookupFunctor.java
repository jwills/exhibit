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

import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibitDescriptor;

public class LookupFunctor implements Functor {

  private final String frame;

  public LookupFunctor(String frame) {
    this.frame = frame;
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor descriptor) {
    if(!descriptor.frames().containsKey(frame)){
      throw new IllegalArgumentException("Frame " + frame + " not present in descriptor " + descriptor.toString());
    }
    return SimpleExhibitDescriptor.of(frame, descriptor.frames().get(frame));
  }

  @Override
  public void cleanup() {
  }

  @Override
  public Exhibit apply(Exhibit exhibit) {
    if(!exhibit.frames().containsKey(frame)){
      throw new IllegalArgumentException("Frame " + frame + " not present in exhibit " + exhibit.toString());
    }
    return SimpleExhibit.of(frame, exhibit.frames().get(frame));
  }
}
