/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JSCalculatorTest {

  ObsDescriptor res1 = SimpleObsDescriptor.builder()
      .doubleField("a")
      .booleanField("b")
      .build();

  @Test
  public void testBasic() throws Exception {
    JSCalculator jsc = new JSCalculator(res1, "var a = function() { return {a: 2, b: true}; }; return a()");
    ObsDescriptor od = SimpleObsDescriptor.builder().intField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame));
    jsc.initialize(e.descriptor());
    Obs res = jsc.apply(e);
    assertEquals(SimpleObs.of(res1, 2.0, true), res);
    jsc.cleanup();
  }
}
