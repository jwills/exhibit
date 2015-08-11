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

import com.cloudera.exhibit.core.composite.CompositeExhibit;
import com.cloudera.exhibit.core.composite.CompositeObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompositeTest {

  public static final ObsDescriptor F1_DESC = SimpleObsDescriptor.builder()
      .doubleField("v0")
      .stringField("v1")
      .build();

  public static final ObsDescriptor F2_DESC = SimpleObsDescriptor.builder()
      .intField("x")
      .build();

  @Test
  public void testCompositeObsDescriptor() throws Exception {
    CompositeObsDescriptor cod = new CompositeObsDescriptor(ImmutableList.of(F1_DESC, F2_DESC));
    assertEquals(3, cod.size());
    assertEquals(1, cod.indexOf("v1"));
    assertEquals(2, cod.indexOf("x"));
    assertEquals(-1, cod.indexOf("v3"));
    assertEquals(new ObsDescriptor.Field("x", FieldType.INTEGER), cod.get(2));
  }

  @Test
  public void testCompositeObsDescriptorWithEmpty() throws Exception {
    CompositeObsDescriptor cod = new CompositeObsDescriptor(ImmutableList.of(ObsDescriptor.EMPTY, F1_DESC));
    assertEquals(2, cod.size());
    assertEquals(0, cod.indexOf("v0"));
    assertEquals(1, cod.indexOf("v1"));
    assertEquals(-1, cod.indexOf("q"));
    assertEquals(new ObsDescriptor.Field("v1", FieldType.STRING), cod.get(1));
    assertEquals(new ObsDescriptor.Field("v0", FieldType.DOUBLE), cod.get(0));
  }

  @Test
  public void testCompositeExhibitFrames() throws Exception {
    Frame sf1 = SimpleFrame.of(
        SimpleObs.of(F1_DESC, 1.0, "a")
        , SimpleObs.of(F1_DESC, 2.0, "b"));

    Frame sf2 =  SimpleFrame.of(
        SimpleObs.of(F2_DESC, 1)
        , SimpleObs.of(F2_DESC, 2));

    Exhibit se1 = SimpleExhibit.of("df0", sf1);
    Exhibit se2 = SimpleExhibit.of("df1", sf2);
    Exhibit ce = CompositeExhibit.of(se1, se2);
    assertEquals(0, ce.attributes().descriptor().size());
    assertEquals(0, ce.vectors().size());
    assertEquals(2, ce.frames().size());
    assertEquals(sf1, ce.frames().get("df0"));
    assertEquals(sf2, ce.frames().get("df1"));
  }

  @Test
  public void testCompositeExhibitVectors() throws Exception {
    Vector v1 = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Vector v2 = VectorBuilder.ints(ImmutableList.<Object>of(1, 2, 3));

    Exhibit se1 = SimpleExhibit.of("v1", v1);
    Exhibit se2 = SimpleExhibit.of("v2", v2);
    Exhibit ce = CompositeExhibit.of(se1, se2);
    assertEquals(0, ce.attributes().descriptor().size());
    assertEquals(0, ce.frames().size());
    assertEquals(2, ce.vectors().size());
    assertEquals(v1, ce.vectors().get("v1"));
    assertEquals(v2, ce.vectors().get("v2"));
  }

  @Test
  public void testInvalidComposition() throws Exception {
    Vector v1 = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Frame sf1 = SimpleFrame.of(SimpleObs.of(F1_DESC, 1.0, "a")
        , SimpleObs.of(F1_DESC, 2.0, "b"));

    try{
      Exhibit se1 = SimpleExhibit.of("f0", sf1);
      Exhibit ce = CompositeExhibit.of(se1, se1);
      fail("Invalid composition of same named frames succeeded");
    } catch(Exception e) {}

    try{
      Exhibit se1 = SimpleExhibit.of("v0", v1);
      Exhibit ce = CompositeExhibit.of(se1, se1);
      fail("Invalid composition of same named vectors succeeded");
    } catch(Exception e) {}

  }
}
