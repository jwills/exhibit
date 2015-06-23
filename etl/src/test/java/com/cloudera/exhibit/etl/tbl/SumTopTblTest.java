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
package com.cloudera.exhibit.etl.tbl;

import com.cloudera.exhibit.avro.AvroObsDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SumTopTblTest {

  private Schema schema = SchemaBuilder.record("test").fields()
      .optionalString("key")
      .optionalDouble("a")
      .optionalInt("b")
      .endRecord();
  private Map<CharSequence, GenericData.Record> cv = Maps.newHashMap();
  private Map<String, String> values = Maps.newHashMap();

  @Before
  public void setUp() throws Exception {
    GenericData.Record one = new GenericData.Record(schema);
    one.put("key", "x");
    one.put("a", 17.0);
    one.put("b", 12);
    GenericData.Record two = new GenericData.Record(schema);
    two.put("key", "y");
    two.put("a", -1.2);
    two.put("b", 32);
    cv.put("one", one);
    cv.put("two", two);

    values.put("key", "key");
    values.put("a", "a");
    values.put("b", "b");
  }

  @Test
  public void testOrder() {
    Map<String, Object> opts = Maps.newHashMap();
    opts.put("by", "key");
    opts.put("order", "a + b");
    opts.put("limit", 1);
    SumTopTbl stt = new SumTopTbl(values, opts);
    ObsDescriptor od = new AvroObsDescriptor(schema);

    SchemaProvider sp = stt.getSchemas(od, 0, 0);
    stt.initialize(sp);
    List<Map.Entry<CharSequence, GenericData.Record>> elem = stt.sort(stt.filter(cv));
    assertEquals(2, elem.size());
    assertEquals("y", elem.get(0).getValue().get("key"));
    assertEquals("x", elem.get(1).getValue().get("key"));
  }

  @Test
  public void testFilter() {
    Map<String, Object> opts = Maps.newHashMap();
    opts.put("by", "key");
    opts.put("order", "a * b");
    opts.put("limit", 1);
    opts.put("keep", "a > 0 && b > 0");
    SumTopTbl stt = new SumTopTbl(values, opts);
    ObsDescriptor od = new AvroObsDescriptor(schema);

    SchemaProvider sp = stt.getSchemas(od, 0, 0);
    stt.initialize(sp);
    List<Map.Entry<CharSequence, GenericData.Record>> elem = stt.filter(cv);
    assertEquals(1, elem.size());
    assertEquals("x", elem.get(0).getValue().get("key"));
  }
}
