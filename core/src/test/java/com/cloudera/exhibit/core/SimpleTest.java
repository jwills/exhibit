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

import com.cloudera.exhibit.core.composite.CompositeObsDescriptor;
import com.cloudera.exhibit.core.simple.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class SimpleTest {

  public static final ObsDescriptor ATTR_DESC = SimpleObsDescriptor.builder()
      .booleanField("a")
      .stringField("b")
      .intField("c")
      .build();

  public static final Vector VECTOR
      = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));

  public static final ExhibitDescriptor EXHIBIT_DESCRIPTOR
      = new SimpleExhibitDescriptor(ATTR_DESC
        , ImmutableMap.of("df", ATTR_DESC)
        , ImmutableMap.of("vec", VECTOR.getType()));

  @Test
  public void testSimpleObsDescriptor() throws Exception{
    assertEquals(3, ATTR_DESC.size());
    assertEquals(1, ATTR_DESC.indexOf("b"));
    assertEquals(-1, ATTR_DESC.indexOf("q"));
    assertEquals(new ObsDescriptor.Field("c", FieldType.INTEGER), ATTR_DESC.get(2));
    assertEquals(SimpleObsDescriptor.builder().booleanField("a").stringField("b").intField("c").build(), ATTR_DESC);
    assertEquals(new CompositeObsDescriptor(ImmutableList.of(ATTR_DESC)), ATTR_DESC);
    assertNotSame(SimpleObsDescriptor.of("anything", FieldType.BOOLEAN), ATTR_DESC);
  }

  @Test
  public void testSimpleObs() throws Exception {
    SimpleObs so = SimpleObs.of(ATTR_DESC, true, "test", 1);
    assertEquals(true, so.get("a"));
    assertEquals("test", so.get(1));
    assertEquals(1, so.get("c"));
    assertEquals(so, so);

    SimpleObs soOther = SimpleObs.of(ATTR_DESC, false, "frame", 2);
    assertNotSame(so, soOther);
  }

  @Test
  public void testSimpleExhibitDescriptor() throws Exception{
    assertEquals(3, EXHIBIT_DESCRIPTOR.attributes().size());
    assertEquals(3, EXHIBIT_DESCRIPTOR.frames().get("df").size());
    assertEquals(FieldType.DOUBLE, EXHIBIT_DESCRIPTOR.vectors().get("vec"));
    assertEquals(ATTR_DESC, EXHIBIT_DESCRIPTOR.attributes());
    assertEquals(ATTR_DESC, EXHIBIT_DESCRIPTOR.frames().get("df"));

    assertEquals(EXHIBIT_DESCRIPTOR, EXHIBIT_DESCRIPTOR);
    assertNotSame(SimpleExhibitDescriptor.of("df", ATTR_DESC), EXHIBIT_DESCRIPTOR);
  }

  @Test
  public void testExhibitDescriptorClone() throws Exception {
    ExhibitDescriptor ed = EXHIBIT_DESCRIPTOR.clone();
    assertEquals(EXHIBIT_DESCRIPTOR, ed);
    ed.frames().put("df-dupe", ATTR_DESC);
    assertNotSame(EXHIBIT_DESCRIPTOR, ed);
  }

  @Test
  public void testSimpleFrame() throws Exception {
    SimpleObs so1 = SimpleObs.of(ATTR_DESC, true, "test", 1);
    SimpleObs so2 = SimpleObs.of(ATTR_DESC, false, "frame", 2);
    Frame f = SimpleFrame.of(so1, so2);
    assertEquals(ATTR_DESC, f.descriptor());
    assertEquals(2, f.size());
    assertEquals(so2, f.get(1));

    Frame fSame = SimpleFrame.of(so1, so2);
    assertEquals(f, fSame);
    Frame fDiff = SimpleFrame.of(so2, so1);
    assertNotSame(f, fDiff);
  }

  @Test
  public void testSimpleExhibit() throws Exception {
    SimpleObs so1 = SimpleObs.of(ATTR_DESC, true, "test", 1);
    SimpleObs so2 = SimpleObs.of(ATTR_DESC, false, "frame", 2);
    Frame f1 = SimpleFrame.of(so1, so2);
    Frame f2 = SimpleFrame.of(so2, so1);
    Exhibit e = new SimpleExhibit(so1, ImmutableMap.of("df", f1), ImmutableMap.of("vec", VECTOR));
    assertEquals(e,e);
    assertEquals(EXHIBIT_DESCRIPTOR, e.descriptor());
    assertEquals(2, e.frames().get("df").size());
    assertEquals(3, e.vectors().get("vec").size());
    Exhibit eOther;
    eOther = new SimpleExhibit(so1, ImmutableMap.of("df", f2), ImmutableMap.of("vec", VECTOR));
    assertEquals(eOther,eOther);
    assertNotSame(e, eOther);
    eOther = new SimpleExhibit(so1, Collections.<String, Frame>emptyMap(), ImmutableMap.of("vec", VECTOR));
    assertEquals(eOther,eOther);
    assertNotSame(e, eOther);
    eOther = new SimpleExhibit(so1, ImmutableMap.of("df", f1), ImmutableMap.<String, Vector>of());
    assertEquals(eOther,eOther);
    assertNotSame(e, eOther);
    eOther = new SimpleExhibit(Obs.EMPTY, ImmutableMap.of("df", f1), ImmutableMap.of("vec", VECTOR));
    assertEquals(eOther,eOther);
    assertNotSame(e, eOther);
  }

}
