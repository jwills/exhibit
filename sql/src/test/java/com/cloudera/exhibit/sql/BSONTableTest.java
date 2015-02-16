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

import com.cloudera.exhibit.core.ObsDescriptor.FieldType;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.mongodb.BSONFrame;
import com.cloudera.exhibit.mongodb.BSONObsDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import org.bson.BSONObject;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BSONTableTest {
  @Test
  public void testEmpty() throws Exception {
    BSONObsDescriptor d = new BSONObsDescriptor(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(FieldType.INTEGER, FieldType.DOUBLE, FieldType.STRING));
    BSONFrame bst = new BSONFrame(d, ImmutableList.<BSONObject>of());
    String[] queries = new String[]{
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a"
    };
    SQLCalculator calc = new SQLCalculator(queries);
    ResultSet rs = calc.apply(SimpleExhibit.of("t1", bst));
    assertFalse(rs.next());
  }

  @Test
  public void testBasic() throws Exception {
    BSONObsDescriptor d = new BSONObsDescriptor(
            ImmutableList.of("a", "b", "c"),
            ImmutableList.of(FieldType.INTEGER, FieldType.DOUBLE, FieldType.STRING));
    BSONFrame bst = new BSONFrame(d, ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 1.0, "c", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "c", "bar"))));

    String[] queries = new String[] {
      "select a, sum(b) as sumb from t1 where c = 'foo' group by a"
    };

    Exhibit exhibit = SimpleExhibit.of("t1", bst);
    ResultSet rs = (new SQLCalculator(queries)).apply(exhibit);
    assertTrue(rs.next());
    assertEquals(1729, rs.getInt("a"));
    assertEquals(1.0, rs.getDouble("sumb"), 0.001);
    assertFalse(rs.next());
  }

  @Test
  public void testTempTables() throws Exception {
    BSONObsDescriptor d = new BSONObsDescriptor(
            ImmutableList.of("a", "b", "c"),
            ImmutableList.of(FieldType.INTEGER, FieldType.DOUBLE, FieldType.STRING));
    BSONFrame bst = new BSONFrame(d, ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 1.0, "c", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "c", "bar"))));

    String[] queries = new String[] {
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a",
        "select sumb + 1 as added from last"
    };

    Exhibit exhibit = SimpleExhibit.of("t1", bst);
    long start = System.currentTimeMillis();
    SQLCalculator calc = new SQLCalculator(queries);
    ResultSet rs = calc.apply(exhibit);
    System.out.println("First = " + (System.currentTimeMillis() - start));
    assertTrue(rs.next());
    assertEquals(2.0, rs.getDouble("added"), 0.001);
    assertFalse(rs.next());

    start = System.currentTimeMillis();
    bst = new BSONFrame(d, ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1728, "b", 2.0, "c", "foo"))));
    rs = calc.apply(SimpleExhibit.of("t1", bst));
    System.out.println("Second = " + (System.currentTimeMillis() - start));
    assertTrue(rs.next());
    assertEquals(3.0, rs.getDouble("added"), 0.001);
  }

  @Test
  public void testColumnMappings() throws Exception {
    BSONObsDescriptor d = new BSONObsDescriptor(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of(FieldType.INTEGER, FieldType.DOUBLE, FieldType.STRING),
        ImmutableMap.of("c", "d"));
    List<BasicDBObject> records = ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 1.0, "d", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "d", "bar"))
    );
    BSONFrame bst = new BSONFrame(d, records);
    String[] queries = new String[] {
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a",
    };

    Exhibit exhibit = SimpleExhibit.of("t1", bst);
    ResultSet rs = (new SQLCalculator(queries)).apply(exhibit);
    assertTrue(rs.next());
    assertEquals(1729, rs.getInt("a"));
    assertEquals(1.0, rs.getDouble("sumb"), 0.001);
    assertFalse(rs.next());
  }
}
