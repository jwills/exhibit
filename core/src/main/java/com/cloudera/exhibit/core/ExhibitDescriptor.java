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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public abstract class ExhibitDescriptor implements Serializable, Cloneable {
  abstract public ObsDescriptor attributes();
  abstract public Map<String, ObsDescriptor> frames();
  abstract public Map<String, FieldType> vectors();

  abstract public ExhibitDescriptor clone();

  @Override
  public int hashCode(){
    return attributes().hashCode()
         + 3 * Objects.hash(frames())
         + 5 * Objects.hash(vectors());
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ExhibitDescriptor)) {
      return false;
    }
    ExhibitDescriptor otherDescriptor = (ExhibitDescriptor)other;
    return attributes().equals(otherDescriptor.attributes())
        && vectors().equals(otherDescriptor.vectors())
        && frames().equals(otherDescriptor.frames());
  }
}
