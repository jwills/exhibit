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
package com.cloudera.exhibit.etl.tbl;

import com.cloudera.exhibit.avro.AvroObs;
import com.cloudera.exhibit.avro.AvroObsDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RatioTblTest {
  private Schema schema = SchemaBuilder.record("test").fields()
          .optionalDouble("a")
          .optionalInt("b")
          .endRecord();
  private Map<String, String> values = Maps.newHashMap();

  @Before
  public void setUp() throws Exception {

    values.put("out", "out");
  }

  @Test
  public void testRatio() {
    GenericData.Record one = new GenericData.Record(schema);
    one.put("a", 10.0);
    one.put("b", 100);
    GenericData.Record two = new GenericData.Record(schema);
    two.put("a", 40.0);
    two.put("b", 100);

    Map<String, Object> opts = Maps.newHashMap();
    opts.put("numerator", "a");
    opts.put("denominator", "b");
    RatioTbl rt = new RatioTbl(values, opts);
    ObsDescriptor od = new AvroObsDescriptor(schema);

    SchemaProvider sp = rt.getSchemas(od, 0, 0);
    rt.initialize(sp);
    GenericData.Record r0 = rt.getValue();
    assertEquals(0.0, (Double) r0.get("ratio"), 1e-6);
    assertEquals(0.0, (Double) r0.get("denominator"), 1e-6);

    rt.add(new AvroObs(od, one));
    GenericData.Record r1 = rt.getValue();
    assertEquals(0.10, (Double) r1.get("ratio"), 1e-6);
    assertEquals(100.0, (Double) r1.get("denominator"), 1e-6);

    rt.add(new AvroObs(od, two));
    GenericData.Record r2 = rt.getValue();
    assertEquals(0.25, (Double) r2.get("ratio"), 1e-6);
    assertEquals(200.0, (Double) r2.get("denominator"), 1e-6);

    GenericData.Record m = rt.merge(r1, r2);
    assertEquals(0.2, (Double) m.get("ratio"), 1e-6);
    assertEquals(300.0, (Double) m.get("denominator"), 1e-6);

    GenericData.Record f = rt.finalize(m).get(0);
    assertEquals(0.2, (Double) f.get("out"), 1e-6);
  }
}
