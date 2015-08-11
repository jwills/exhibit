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
package com.cloudera.exhibit.javascript;

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.simple.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class JSFunctorTest {

  ObsDescriptor res1 = SimpleObsDescriptor.builder()
      .doubleField("a")
      .booleanField("b")
      .build();

  @Test
  public void testDescriptor() throws Exception {
    {
      JSFunctor jsc = new JSFunctor("0.0");
      ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
      jsc.cleanup();
      assertEquals(0, ed.frames().size());
      assertEquals(0, ed.vectors().size());
      assertEquals(1, ed.attributes().size());
      assertEquals(SimpleObsDescriptor.of(JsFunctorConstants.DEFAULT_FIELD_NAME, FieldType.DOUBLE), ed.attributes());
    }
    {
      JSFunctor jsc = new JSFunctor("[1,2,3]");
      ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
      jsc.cleanup();
      assertEquals(0, ed.frames().size());
      assertEquals(0, ed.attributes().size());
      assertEquals(1, ed.vectors().size());
      assertTrue(ed.vectors().containsKey(JsFunctorConstants.DEFAULT_FIELD_NAME));
      assertEquals(FieldType.DOUBLE, ed.vectors().get(JsFunctorConstants.DEFAULT_FIELD_NAME));
    }
    {
      JSFunctor jsc = new JSFunctor("return {a: 0, b: \"Prateek\"};");
      ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
      jsc.cleanup();
      assertEquals(0, ed.frames().size());
      assertEquals(2, ed.attributes().size());
      assertEquals(0, ed.vectors().size());
      assertEquals(SimpleObsDescriptor.of("a", FieldType.DOUBLE, "b", FieldType.STRING), ed.attributes());
    }
    {
      JSFunctor jsc = new JSFunctor("return {a: 0, df: [{a: 0, b: 1}]};");
      ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
      jsc.cleanup();
      assertEquals(1, ed.frames().size());
      assertEquals(1, ed.attributes().size());
      assertEquals(0, ed.vectors().size());
      assertEquals(SimpleObsDescriptor.of("a", FieldType.DOUBLE), ed.attributes());
      assertTrue(ed.frames().containsKey("df"));
      assertEquals(SimpleObsDescriptor.of("a", FieldType.DOUBLE, "b", FieldType.DOUBLE), ed.frames().get("df"));
    }
    {
      JSFunctor jsc = new JSFunctor("return {a: 0, vec: [0,1]};");
      ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
      jsc.cleanup();
      assertEquals(0, ed.frames().size());
      assertEquals(1, ed.attributes().size());
      assertEquals(1, ed.vectors().size());
      assertEquals(SimpleObsDescriptor.of("a", FieldType.DOUBLE), ed.attributes());
      assertTrue(ed.vectors().containsKey("vec"));
      assertEquals(FieldType.DOUBLE, ed.vectors().get("vec"));
    }
    {
      JSFunctor jsc = new JSFunctor("return {a: 0, df: [{a: 0, b: 1}], vec: [true, false]};");
      ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
      jsc.cleanup();
      assertEquals(1, ed.frames().size());
      assertEquals(1, ed.attributes().size());
      assertEquals(1, ed.vectors().size());
      assertEquals(SimpleObsDescriptor.of("a", FieldType.DOUBLE), ed.attributes());
      assertTrue(ed.frames().containsKey("df"));
      assertEquals(SimpleObsDescriptor.of("a", FieldType.DOUBLE, "b", FieldType.DOUBLE), ed.frames().get("df"));
      assertTrue(ed.vectors().containsKey("vec"));
      assertEquals(FieldType.BOOLEAN, ed.vectors().get("vec"));
    }
    {
      JSFunctor jsc = new JSFunctor("return {a: 0, df: {a: 0, b: 1}};");
      try{
        ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
        fail("exhibits are not allowed to be nested");
      } catch (Exception e){}
      jsc.cleanup();
    }
    {
      JSFunctor jsc = new JSFunctor("return {df: [{a: 0, b: 1}, 0]};");
      try{
        ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
        jsc.cleanup();
        fail("frame records need to be homogenous");
      } catch (Exception e){}
      jsc.cleanup();
    }
    {
      JSFunctor jsc = new JSFunctor("return [1, true];");
      try{
        ExhibitDescriptor ed = jsc.initialize(SimpleExhibitDescriptor.EMPTY);
        jsc.cleanup();
        fail("vector records need to be homogenous");
      } catch (Exception e){}
      jsc.cleanup();
    }
  }


  @Test
  public void testObs() throws Exception {
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame));
    JSFunctor jsc = new JSFunctor("var a = function() { return {a: df[0].a, b: true}; }; return a()");
    jsc.initialize(e.descriptor());
    Exhibit ret = jsc.apply(e);
    jsc.cleanup();
    assertEquals(2, ret.attributes().size());
    assertEquals(0, ret.frames().size());
    assertEquals(0, ret.vectors().size());
    Obs res = ret.attributes();
    assertEquals(SimpleObs.of(res1, 17.0, true), res);
  }

  @Test
  public void testVector() throws Exception {
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame), ImmutableMap.of("v1", vector));
    JSFunctor jsc = new JSFunctor("var a = function() { return {a: [true, false], b: v1}; }; return a()");

    ExhibitDescriptor ed;
    try {
      ed = jsc.initialize(e.descriptor());
    } catch (Exception ex){
      jsc.cleanup();
      throw ex;
    }
    assertEquals(0, ed.frames().size());
    assertEquals(0, ed.attributes().size());
    assertEquals(2, ed.vectors().size());
    assertTrue(ed.vectors().containsKey("a"));
    assertEquals(FieldType.BOOLEAN, ed.vectors().get("a"));
    assertTrue(ed.vectors().containsKey("b"));
    assertEquals(FieldType.DOUBLE, ed.vectors().get("b"));

    Exhibit ret;
    try {
      ret = jsc.apply(e);
    } catch (Exception ex){
      jsc.cleanup();
      throw ex;
    }
    assertTrue(ret.vectors().containsKey("b"));
    assertEquals(FieldType.DOUBLE, ret.vectors().get("b").getType());
    assertEquals(vector, ret.vectors().get("b"));
    assertEquals(0, ret.attributes().size());
    assertEquals(0, ret.frames().size());
    assertEquals(2, ret.vectors().size());
    assertTrue(ret.vectors().containsKey("a"));
    assertEquals(FieldType.BOOLEAN, ret.vectors().get("a").getType());
    assertEquals(VectorBuilder.bools(true, false), ret.vectors().get("a"));
    jsc.cleanup();
  }

  @Test
  public void testArray() throws Exception {
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame));
    JSFunctor jsc = new JSFunctor("var a = function() { return [{a: df[0].a, b: true}]; }; return a()");
    jsc.initialize(e.descriptor());
    Exhibit ret = jsc.apply(e);
    jsc.cleanup();
    assertEquals(0, ret.attributes().size());
    assertEquals(0, ret.vectors().size());
    assertEquals(1, ret.frames().size());
    assertTrue(ret.frames().containsKey(JsFunctorConstants.DEFAULT_FIELD_NAME));
    assertEquals(SimpleFrame.of(SimpleObs.of(res1, 17.0, true)),
        ret.frames().get(JsFunctorConstants.DEFAULT_FIELD_NAME));
  }

  @Test
  public void testLength() throws Exception {
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame));
    JSFunctor jsc = new JSFunctor("df.length");
    jsc.initialize(e.descriptor());
    Exhibit ret = jsc.apply(e);
    jsc.cleanup();
    assertEquals(1, ret.attributes().size());
    assertEquals(0, ret.vectors().size());
    assertEquals(0, ret.frames().size());
    assertEquals(0, ret.attributes().descriptor().indexOf(JsFunctorConstants.DEFAULT_FIELD_NAME));
    assertEquals(SimpleObs.of( SimpleObsDescriptor.of(
        JsFunctorConstants.DEFAULT_FIELD_NAME, FieldType.DOUBLE), 2.0), ret.attributes());
  }

  @Test
  public void testEmptyVector() throws Exception {
    ExhibitDescriptor ed = SimpleExhibitDescriptor.EMPTY;
    Exhibit e = Exhibits.defaultValues(ed);
    JSFunctor jsc = new JSFunctor("[]");
    ExhibitDescriptor returnedDescriptor = jsc.initialize(ed);
    assertEquals(0, returnedDescriptor.frames().size());
    assertEquals(0, returnedDescriptor.attributes().size());
    assertEquals(1, returnedDescriptor.vectors().size());
    assertTrue(returnedDescriptor.vectors().containsKey(JsFunctorConstants.DEFAULT_FIELD_NAME));
    assertEquals(FieldType.DOUBLE, returnedDescriptor.vectors().get(JsFunctorConstants.DEFAULT_FIELD_NAME));
    jsc.cleanup();
  }

  @Test
  public void testFrame() throws Exception {
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame), ImmutableMap.of("v1", vector));
    JSFunctor jsc = new JSFunctor("return {df: df};");

    ExhibitDescriptor ed;
    try {
      ed = jsc.initialize(e.descriptor());
    } catch (Exception ex){
      jsc.cleanup();
      throw ex;
    }
    assertEquals(1, ed.frames().size());
    assertEquals(0, ed.attributes().size());
    assertEquals(0, ed.vectors().size());
    assertTrue(ed.frames().containsKey("df"));
    assertEquals(od, ed.frames().get("df"));

    Exhibit ret;
    try {
      ret = jsc.apply(e);
    } catch (Exception ex){
      jsc.cleanup();
      throw ex;
    }
    assertEquals(0, ret.attributes().size());
    assertEquals(1, ret.frames().size());
    assertEquals(0, ret.vectors().size());
    assertTrue(ret.frames().containsKey("df"));
    assertEquals(frame, ret.frames().get("df"));
    jsc.cleanup();
  }
}
