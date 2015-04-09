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

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SumTblTest {

  static Schema DB = Schema.create(Schema.Type.DOUBLE);
  static Schema DB_NULL = Schema.createUnion(ImmutableList.of(DB, Schema.create(Schema.Type.NULL)));
  static Schema REC1 = Schema.createRecord(ImmutableList.of(
      new Schema.Field("v1", DB, "", null),
      new Schema.Field("v2", DB_NULL, "", null)));

  @Test
  public void testSumTbl() throws Exception {
    Tbl st = new SumTbl(0, DB);
    assertEquals(29.0, st.merge(null, 29.0));
    assertEquals(1729.0, st.merge(1700.00, 29.0));
    assertEquals(1729.0, st.finalize(1729.0));
  }

  @Test
  public void testSumTblNull() throws Exception {
    Tbl st = new SumTbl(0, DB_NULL);
    assertEquals(29.0, st.merge(null, 29.0));
    assertEquals(1729.0, st.merge(1700.00, 29.0));
    assertEquals(1729.0, st.finalize(1729.0));
  }

  @Test
  public void testSumRec() throws Exception {
    GenericRecord gr1 = new GenericData.Record(REC1);
    gr1.put("v1", 17.0);
    gr1.put("v2", null);
    GenericRecord gr2 = new GenericData.Record(REC1);
    gr2.put("v1", 17.0);
    gr2.put("v2", 29.0);
    Tbl st = new SumTbl(0, REC1);
    assertEquals(gr2, st.merge(null, gr2));
    assertEquals(gr1, st.merge(gr1, gr2));
    assertEquals(34.0, gr1.get("v1"));
    assertEquals(29.0, gr1.get("v2"));
  }
}
