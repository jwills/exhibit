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
package com.cloudera.exhibit.octave;

import java.io.Serializable;
import java.util.Set;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.vector.VectorUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class OctaveCalculator implements Calculator, Serializable{
  private OctaveFunctor functor;
  private String varName;

  OctaveCalculator(String script) throws OctaveScriptFormatException {
    this(null, script);
  }

  OctaveCalculator(ExhibitDescriptor descriptor, String script) throws OctaveScriptFormatException {
    this.functor = new OctaveFunctor(descriptor, script);
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor input) {
    ExhibitDescriptor res = functor.initialize(input);
    if (!res.attributes().isEmpty()) {
      return res.attributes();
    } else if (!res.vectors().isEmpty()) {
      this.varName = Iterables.getOnlyElement(res.vectors().keySet());
      return VectorUtils.asObsDescriptor(res.vectors().get(varName));
    } else if (!res.frames().isEmpty()) {
      this.varName = Iterables.getOnlyElement(res.frames().keySet());
      return res.frames().get(varName);
    }
    throw new IllegalArgumentException("Could not determine return variable for octave calculation");
  }

  @Override
  public void cleanup() {
    functor.cleanup();
  }

  @Override
  public Iterable<Obs> apply(Exhibit input) {
    return extract(functor.apply(input));
  }


  private Iterable<Obs> extract(Exhibit result) {
    if (result.frames().containsKey(varName)) {
      return result.frames().get(varName);
    } else if (result.vectors().containsKey(varName)) {
      return VectorUtils.asFrame(result.vectors().get(varName));
    } else {
      return ImmutableList.of(result.attributes());
    }
  }

}
