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

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.simple.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OctaveFunctorTest {

  @Test
  public void testObs() throws Exception {
    ExhibitDescriptor expectedDescriptor = new SimpleExhibitDescriptor(
        SimpleObsDescriptor.builder().doubleField("d").build(),
        Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    OctaveFunctor osc = new OctaveFunctor("d = sum(v(:));");
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Exhibit e = SimpleExhibit.of("v", vector);
    ExhibitDescriptor resultDescriptor = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", expectedDescriptor, resultDescriptor);
    Exhibit result = osc.apply(e);
    Obs res = result.attributes();
    assertEquals(1, res.size());
    assertEquals(6.0, res.get(0));
    osc.cleanup();
  }

  @Test
  public void testFrame() throws Exception {
    ExhibitDescriptor expectedDescriptor = SimpleExhibitDescriptor.of("d",
        SimpleObsDescriptor
            .builder()
            .doubleField("d$0")
            .doubleField("d$1")
            .doubleField("d$2")
            .build()
    );

    String func = "d = [df v1];";
    OctaveFunctor osc = new OctaveFunctor(func);
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").doubleField("b").build();
    Obs obs = SimpleObs.of(od, 1.0, 2.0);
    Obs one = SimpleObs.of(od, 1.0, 2.0);
    Obs two = SimpleObs.of(od, 4.0, 5.0);
    Obs three = SimpleObs.of(od, 7.0, 8.0);
    Frame frame = SimpleFrame.of(one, two, three);
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(3.0, 6.0, 9.0));
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame), ImmutableMap.of("v1", vector));
    ExhibitDescriptor rd = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", expectedDescriptor, rd);
    Exhibit resultExhibit = osc.apply(e);
    assertEquals(0, resultExhibit.attributes().size());
    assertEquals(0, resultExhibit.vectors().size());
    assertEquals(1, resultExhibit.frames().size());
    assertTrue(resultExhibit.frames().containsKey("d"));
    Frame results = resultExhibit.frames().get("d");
    assertEquals(3, results.size());
    assertEquals(1.0, results.get(0).get(0));
    assertEquals(2.0, results.get(0).get(1));
    assertEquals(3.0, results.get(0).get(2));
    assertEquals(4.0, results.get(1).get(0));
    assertEquals(5.0, results.get(1).get(1));
    assertEquals(6.0, results.get(1).get(2));
    osc.cleanup();
  }

  @Test
  public void testFunction() throws Exception {
    ObsDescriptor FRAME_DESC = SimpleObsDescriptor.builder().doubleField("d$0").doubleField("d$1").build();
    ExhibitDescriptor EXHIBIT_DESC = SimpleExhibitDescriptor.of("d", FRAME_DESC);

    String func = ""
        + "function res = my_func(a)\n"
        + " res = 2 * a;\n"
        + "endfunction\n"
        + "d = [v my_func(v)];";
    OctaveFunctor osc = new OctaveFunctor(func);
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Exhibit e = SimpleExhibit.of("v", vector);
    ExhibitDescriptor rd = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", EXHIBIT_DESC, rd);
    Exhibit resultExhibit = osc.apply(e);
    assertEquals(0, resultExhibit.attributes().size());
    assertEquals(0, resultExhibit.vectors().size());
    assertEquals(1, resultExhibit.frames().size());
    assertTrue(resultExhibit.frames().containsKey("d"));
    Frame results = resultExhibit.frames().get("d");
    assertEquals(3, results.size());
    assertEquals(1.0, results.get(0).get(0));
    assertEquals(2.0, results.get(0).get(1));
    assertEquals(2.0, results.get(1).get(0));
    assertEquals(4.0, results.get(1).get(1));
    assertEquals(3.0, results.get(2).get(0));
    assertEquals(6.0, results.get(2).get(1));
    osc.cleanup();
  }

  @Test
  public void testLong() throws Exception {
    ExhibitDescriptor EXHIBIT_DESC = SimpleExhibitDescriptor.of("d", FieldType.DOUBLE);
    String func =  "d = v;";
    OctaveFunctor osc = new OctaveFunctor(func);
    Vector vector = VectorBuilder.longs(ImmutableList.<Object>of(1L, 2L, 3L));
    Exhibit e = SimpleExhibit.of("v", vector);
    ExhibitDescriptor resultDescriptor = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", EXHIBIT_DESC, resultDescriptor);
    Exhibit resultExhibit = osc.apply(e);
    assertEquals(0, resultExhibit.attributes().size());
    assertEquals(1, resultExhibit.vectors().size());
    assertEquals(0, resultExhibit.frames().size());
    assertTrue(resultExhibit.vectors().containsKey("d"));
    Vector results = resultExhibit.vectors().get("d");
    assertEquals(3, results.size());
    assertEquals(1.0, results.get(0));
    assertEquals(2.0, results.get(1));
    assertEquals(3.0, results.get(2));
    osc.cleanup();
  }
}