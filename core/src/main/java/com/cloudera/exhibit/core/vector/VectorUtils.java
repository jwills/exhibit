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
package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class VectorUtils {

  public static ObsDescriptor asObsDescriptor(String name, FieldType vectorType) {
    return SimpleObsDescriptor.of(name, vectorType);
  }

  public static Frame asFrame(String name, Vec vec) {
    final ObsDescriptor od = asObsDescriptor(name, vec.getType());
    return new SimpleFrame(od, Lists.transform(Lists.newArrayList(vec), new Function<Object, Obs>() {
      @Override
      public Obs apply(Object o) {
        return SimpleObs.of(od, o);
      }
    }));
  }
}
