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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.composite.UpdatableExhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.javascript.JSCalculator;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JSQLTest {

  @Test
  public void testJS2SQL() throws Exception {
    JSCalculator jsc = new JSCalculator("[{id: 123}]");
    ObsDescriptor od = SimpleObsDescriptor.builder().doubleField("a").booleanField("b").build();
    Obs obs = SimpleObs.of(od, 1729, true);
    Obs one = SimpleObs.of(od, 17, true);
    Obs two = SimpleObs.of(od, 12, false);
    Frame frame = SimpleFrame.of(one, two);
    Exhibit e = new SimpleExhibit(obs, ImmutableMap.of("df", frame));
    jsc.initialize(e.descriptor());
    UpdatableExhibit ue = new UpdatableExhibit(e);
    ue.add("jsres", (Frame) jsc.apply(e));

    SQLCalculator sql = SQLCalculator.create(null, "SELECT count(*) suma FROM jsres where id > 0");
    sql.initialize(ue.descriptor());
    Iterable<Obs> res = sql.apply(ue);
    assertEquals(
        new SimpleObs(SimpleObsDescriptor.of("suma", ObsDescriptor.FieldType.LONG), ImmutableList.<Object>of(1L)),
        Iterables.getOnlyElement(res));
  }
}
