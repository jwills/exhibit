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

import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

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
    assertEquals(new SimpleObs(od, ImmutableList.<Object>of(true, false, 17, 29)), obs);
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
    assertEquals(new SimpleObs(od, Lists.<Object>newArrayList(17, null, null, 29)), obs);
  }

  @Test
  public void testPivotId() throws Exception {
    PivotCalculator.Key b = new PivotCalculator.Key("b", ImmutableSet.of("v1", "v2"));
    PivotCalculator pc = new PivotCalculator(TEST_FC, ImmutableList.<String>of("a"), ImmutableList.of(b));
    ObsDescriptor od = pc.initialize(null);
    System.out.println(od);
    Obs e1 = new SimpleObs(od, Lists.<Object>newArrayList(false, null, 29));
    Obs e2 = new SimpleObs(od, Lists.<Object>newArrayList(true, 17, null));
    assertEquals(ImmutableList.of(e1, e2), Lists.newArrayList(pc.apply(null)));
  }
}
