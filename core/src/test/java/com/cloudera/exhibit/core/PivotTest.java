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
package com.cloudera.exhibit.core;

import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Test;

import java.util.List;

public class PivotTest {
  public static final ObsDescriptor DESC = SimpleObsDescriptor.builder()
          .booleanField("a")
          .stringField("b")
          .intField("c")
          .build();

  private static final Calculator TEST_FC = new Calculator() {
    @Override
    public ObsDescriptor initialize(ExhibitDescriptor descriptor) {
      return DESC;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Frame apply(Exhibit exhibit) {
      Obs o1 = new SimpleObs(DESC, ImmutableList.<Object>of(true, "v1", 17));
      Obs o2 = new SimpleObs(DESC, ImmutableList.<Object>of(false, "v2", 29));
      return new SimpleFrame(DESC, ImmutableList.of(o1, o2));
    }
  };

  @Test
  public void testPivotOne() throws Exception {
    PivotCalculator.Key b = new PivotCalculator.Key("b", ImmutableSet.of("v1", "v2"));
    PivotCalculator pc = new PivotCalculator(TEST_FC, ImmutableList.<String>of(), ImmutableList.of(b));
    ObsDescriptor od = pc.initialize(null);
    System.out.println(od);
    Obs obs = Iterables.getOnlyElement(pc.apply(null));
    System.out.println(obs);
  }

  @Test
  public void testPivotTwo() throws Exception {
    List<PivotCalculator.Key> keys = ImmutableList.of(
        new PivotCalculator.Key("a", ImmutableSet.of("true", "false")),
        new PivotCalculator.Key("b", ImmutableSet.of("v1", "v2")));
    PivotCalculator pc = new PivotCalculator(TEST_FC, ImmutableList.<String>of(), keys);
    ObsDescriptor od = pc.initialize(null);
    System.out.println(od);
    Obs obs =  Iterables.getOnlyElement(pc.apply(null));
    System.out.println(obs);
  }
}
