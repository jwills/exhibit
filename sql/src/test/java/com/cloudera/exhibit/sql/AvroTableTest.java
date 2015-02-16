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
package com.cloudera.exhibit.sql;

import com.cloudera.exhibit.avro.AvroFrame;
import com.cloudera.exhibit.avro.AvroObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AvroTableTest {

  Schema schema = SchemaBuilder.record("foo").fields()
      .nullableString("f1", "")
      .optionalBoolean("f2")
      .requiredInt("f3")
      .endRecord();

  @Test
  public void testEmpty() throws Exception {
    AvroObsDescriptor at = new AvroObsDescriptor(schema);
    String[] queries = new String[] {
      "select f2, sum(f3) as sumf3 from t1 where f1 = 'foo' group by f2"
    };
    SQLCalculator calc = new SQLCalculator(queries);
    ResultSet rs = calc.apply(SimpleExhibit.of("t1", new AvroFrame(at)));
    assertFalse(rs.next());
  }

  @Test
  public void testBasic() throws Exception {
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("f1", "foo");
    r1.put("f2", true);
    r1.put("f3", 1729);
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("f1", "for");
    r2.put("f2", true);
    r2.put("f3", 17);
    AvroFrame frame = new AvroFrame(ImmutableList.of(r1, r2));
    String[] queries = new String[] {
        "select f2, sum(f3) as sumf3 from t1 where f1 = 'foo' group by f2"
    };
    SQLCalculator calc = new SQLCalculator(queries);
    ResultSet rs = calc.apply(SimpleExhibit.of("t1", frame));
    assertTrue(rs.next());
    assertEquals(true, rs.getBoolean(1));
    assertEquals(1729, rs.getInt(2));
  }

  @Test
  public void testMissingFields() throws Exception {
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("f1", "foo");
    r1.put("f3", 1729);
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("f2", true);
    r2.put("f3", 17);
    AvroFrame frame = new AvroFrame(ImmutableList.of(r1, r2));
    String[] queries = new String[] {
        "select f2, sum(f3) as sumf3 from t1 where f1 = 'foo' group by f2"
    };
    SQLCalculator calc = new SQLCalculator(queries);
    ResultSet rs = calc.apply(SimpleExhibit.of("t1", frame));
    assertTrue(rs.next());
    assertEquals(false, rs.getBoolean(1));
    assertEquals(1729, rs.getInt(2));
  }
}
