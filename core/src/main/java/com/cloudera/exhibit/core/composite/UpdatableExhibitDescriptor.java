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

import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibitDescriptor;
import com.google.common.collect.Maps;

public class UpdatableExhibitDescriptor extends SimpleExhibitDescriptor {
  public UpdatableExhibitDescriptor(ExhibitDescriptor base) {
    super(base.attributes(), Maps.newHashMap(base.frames()), Maps.newHashMap(base.vectors()));
  }

  public UpdatableExhibitDescriptor addFrame(String name, ObsDescriptor od) {
    frames().put(name, od);
    return this;
  }

  public UpdatableExhibitDescriptor addVector(String name, FieldType vd) {
    vectors().put(name, vd);
    return this;
  }
}
