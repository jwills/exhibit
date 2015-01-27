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
package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.OptiqHelper;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AvroTableTest {

  OptiqHelper helper = new OptiqHelper();

  Schema schema = SchemaBuilder.record("foo").fields()
      .nullableString("f1", "")
      .optionalBoolean("f2")
      .requiredInt("f3")
      .endRecord();

  @Test
  public void testEmpty() throws Exception {
    AvroTable at = new AvroTable(schema);
    String[] queries = new String[] {
      "select f2, sum(f3) as sumf3 from t1 where f1 = 'foo' group by f2"
    };
    helper.initialize(ImmutableList.of(at), queries);
    ResultSet rs = helper.execute();
    assertFalse(rs.next());
  }

  @Test
  public void testBasic() throws Exception {
    AvroTable at = new AvroTable(schema);
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("f1", "foo");
    r1.put("f2", true);
    r1.put("f3", 1729);
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("f1", "for");
    r2.put("f2", true);
    r2.put("f3", 17);
    at.updateValues(ImmutableList.of(r1, r2));
    String[] queries = new String[] {
        "select f2, sum(f3) as sumf3 from t1 where f1 = 'foo' group by f2"
    };
    helper.initialize(ImmutableList.of(at), queries);
    ResultSet rs = helper.execute();
    assertTrue(rs.next());
    assertEquals(true, rs.getBoolean(1));
    assertEquals(1729, rs.getInt(2));
  }

  @Test
  public void testMissingFields() throws Exception {
    AvroTable at = new AvroTable(schema);
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("f1", "foo");
    r1.put("f3", 1729);
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("f2", true);
    r2.put("f3", 17);
    at.updateValues(ImmutableList.of(r1, r2));
    String[] queries = new String[] {
        "select f2, sum(f3) as sumf3 from t1 where f1 = 'foo' group by f2"
    };
    helper.initialize(ImmutableList.of(at), queries);
    ResultSet rs = helper.execute();
    assertTrue(rs.next());
    assertEquals(false, rs.getBoolean(1));
    assertEquals(1729, rs.getInt(2));
  }
}
