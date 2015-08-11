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

import com.cloudera.exhibit.core.simple.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PivotTest {
  public static final String FRAME_NAME = "df";
  public static final ObsDescriptor DESC = SimpleObsDescriptor.builder()
          .booleanField("a")
          .stringField("b")
          .intField("c")
          .build();
  public static final ExhibitDescriptor EXHIBIT_DESCRIPTOR = SimpleExhibitDescriptor.of(FRAME_NAME, DESC);

  private static final Functor TEST_FC = new Functor() {
    @Override
    public ExhibitDescriptor initialize(ExhibitDescriptor descriptor) {
      return EXHIBIT_DESCRIPTOR;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Exhibit apply(Exhibit exhibit) {
      Obs o1 = new SimpleObs(DESC, ImmutableList.<Object>of(true, "v1", 17));
      Obs o2 = new SimpleObs(DESC, ImmutableList.<Object>of(false, "v2", 29));
      return SimpleExhibit.of(FRAME_NAME, new SimpleFrame(DESC, ImmutableList.of(o1, o2)));
    }
  };

  @Test
  public void testPivotOne() throws Exception {
    PivotFunctor.Key b = new PivotFunctor.Key("b", ImmutableSet.of("v1", "v2"));
    PivotFunctor pc = new PivotFunctor(FRAME_NAME, TEST_FC, ImmutableList.<String>of(), ImmutableList.of(b));
    ExhibitDescriptor ed = pc.initialize(null);
    ObsDescriptor od = ed.frames().get(FRAME_NAME);
    System.out.println(od);
    Frame pivotedFrame = pc.apply(null).frames().get(FRAME_NAME);
    assertEquals(1, pivotedFrame.size());
    Obs obs = pivotedFrame.get(0);
    assertEquals(new SimpleObs(od, ImmutableList.<Object>of(true, false, 17, 29)), obs);
  }

  @Test
  public void testPivotTwo() throws Exception {
    List<PivotFunctor.Key> keys = ImmutableList.of(
        new PivotFunctor.Key("a", ImmutableSet.of("true", "false")),
        new PivotFunctor.Key("b", ImmutableSet.of("v1", "v2")));
    PivotFunctor pc = new PivotFunctor(FRAME_NAME, TEST_FC, ImmutableList.<String>of(), keys);
    ExhibitDescriptor ed = pc.initialize(null);
    ObsDescriptor od = ed.frames().get(FRAME_NAME);
    System.out.println(od);
    Frame pivotedFrame = pc.apply(null).frames().get(FRAME_NAME);
    assertEquals(1, pivotedFrame.size());
    Obs obs = pivotedFrame.get(0);
    assertEquals(new SimpleObs(od, Lists.<Object>newArrayList(17, null, null, 29)), obs);
  }

  @Test
  public void testPivotId() throws Exception {
    PivotFunctor.Key b = new PivotFunctor.Key("b", ImmutableSet.of("v1", "v2"));
    PivotFunctor pc = new PivotFunctor(FRAME_NAME, TEST_FC, ImmutableList.<String>of("a"), ImmutableList.of(b));
    ExhibitDescriptor ed = pc.initialize(null);
    ObsDescriptor od = ed.frames().get(FRAME_NAME);
    System.out.println(od);
    Frame pivotedFrame = pc.apply(null).frames().get(FRAME_NAME);
    assertEquals(2, pivotedFrame.size());
    Obs obs1 = pivotedFrame.get(0);
    Obs obs2 = pivotedFrame.get(1);
    Obs e1 = new SimpleObs(od, Lists.<Object>newArrayList(false, null, 29));
    Obs e2 = new SimpleObs(od, Lists.<Object>newArrayList(true, 17, null));
    assertEquals(ImmutableList.of(e1, e2), Lists.newArrayList(obs1, obs2));
  }
}
