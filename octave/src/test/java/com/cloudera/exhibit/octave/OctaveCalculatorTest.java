package com.cloudera.exhibit.octave;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import dk.ange.octave.OctaveEngineFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OctaveCalculatorTest {

  private boolean hasOctave;

  public OctaveCalculatorTest() {
    hasOctave = true;
    try {
      new OctaveEngineFactory().getScriptEngine();
    } catch (Throwable t) {
      hasOctave = false;
    }
  }

  @Test
  public void testObs() throws Exception {
    if (!hasOctave) {
      return;
    }
    ObsDescriptor resultDescriptor = SimpleObsDescriptor.builder().doubleField("d").build();
    OctaveCalculator osc = new OctaveCalculator("d = sum(v(:));");
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Exhibit e = SimpleExhibit.of("v", vector);
    ObsDescriptor rd = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", resultDescriptor, rd);
    Obs res = Iterables.getOnlyElement(osc.apply(e));
    assertEquals(6.0, res.get(0));
    osc.cleanup();
  }

  @Test
  public void testFrame() throws Exception {
    if (!hasOctave) {
      return;
    }
    String func = "d = [df v1];";
    OctaveCalculator osc = new OctaveCalculator(func);
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").doubleField("b").build();
    Obs obs = SimpleObs.of(od, 1.0, 2.0);
    Obs one = SimpleObs.of(od, 1.0, 2.0);
    Obs two = SimpleObs.of(od, 4.0, 5.0);
    Obs three = SimpleObs.of(od, 7.0, 8.0);
    Frame frame = SimpleFrame.of(one, two, three);
    Vec vector = VectorBuilder.doubles(ImmutableList.<Object>of(3.0, 6.0, 9.0));
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame), ImmutableMap.of("v1", vector));
    ObsDescriptor resultDescriptor = SimpleObsDescriptor.builder()
        .doubleField("d$0").doubleField("d$1").doubleField("d$2").build();
    ObsDescriptor rd = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", resultDescriptor, rd);
    List<Obs> results = Lists.newArrayList(osc.apply(e));
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
    if (!hasOctave) {
      return;
    }
    String func = ""
        + "function res = my_func(a)\n"
        + " res = 2 * a;\n"
        + "endfunction\n"
        + "d = [v my_func(v)];";
    OctaveCalculator osc = new OctaveCalculator(func);
    Vector vector = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));
    Exhibit e = SimpleExhibit.of("v", vector);
    ObsDescriptor resultDescriptor = SimpleObsDescriptor.builder().doubleField("d$0").doubleField("d$1").build();
    ObsDescriptor rd = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", resultDescriptor, rd);
    List<Obs> results = Lists.newArrayList(osc.apply(e));
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
    if (!hasOctave) {
      return;
    }
    String func = "d = v;";
    OctaveCalculator osc = new OctaveCalculator(func);
    Vector vector = VectorBuilder.longs(ImmutableList.<Object>of(1L, 2L, 3L));
    Exhibit e = SimpleExhibit.of("v", vector);
    ObsDescriptor resultDescriptor = SimpleObsDescriptor.builder().doubleField("d").build();
    ObsDescriptor rd = osc.initialize(e.descriptor());
    assertEquals("Descriptor matches", resultDescriptor, rd);
    List<Obs> results = Lists.newArrayList(osc.apply(e));
    assertEquals(3, results.size());
    assertEquals(1.0, results.get(0).get(0));
    assertEquals(2.0, results.get(1).get(0));
    assertEquals(3.0, results.get(2).get(0));
    osc.cleanup();
  }
}