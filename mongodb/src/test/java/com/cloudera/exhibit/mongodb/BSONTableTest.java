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
package com.cloudera.exhibit.mongodb;

import com.cloudera.exhibit.core.OptiqHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import net.hydromatic.optiq.Table;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BSONTableTest {
  OptiqHelper helper = new OptiqHelper();

  @Test
  public void testEmpty() throws Exception {
    BSONTable bst = BSONTable.create(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.<Object>of(Integer.class, Double.class, String.class));
    List<BasicDBObject> records = ImmutableList.of();
    bst.updateValues(records);

    String[] queries = new String[]{
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a"
    };

    helper.initialize(ImmutableList.<Table>of(bst), queries);
    Statement stmt = helper.newStatement();
    ResultSet rs = helper.execute(stmt);
    assertFalse(rs.next());
  }

  @Test
  public void testBasic() throws Exception {
    BSONTable bst = BSONTable.create(ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 1.0, "c", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "c", "bar"))));

    String[] queries = new String[] {
      "select a, sum(b) as sumb from t1 where c = 'foo' group by a"
    };

    helper.initialize(ImmutableList.<Table>of(bst), queries);
    Statement stmt = helper.newStatement();
    ResultSet rs = helper.execute(stmt);
    assertTrue(rs.next());
    assertEquals(1729, rs.getInt("a"));
    assertEquals(1.0, rs.getDouble("sumb"), 0.001);
    assertFalse(rs.next());
  }

  @Test
  public void testDefaultValues() throws Exception {
    BSONTable bst = BSONTable.create(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.<Object>of(Integer.class, 1.0, String.class));
    List<BasicDBObject> records = ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "c", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "c", "bar"))
    );
    bst.updateValues(records);

    String[] queries = new String[] {
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a"
    };

    helper.initialize(ImmutableList.<Table>of(bst), queries);
    Statement stmt = helper.newStatement();
    ResultSet rs = helper.execute(stmt);
    assertTrue(rs.next());
    assertEquals(1729, rs.getInt("a"));
    assertEquals(1.0, rs.getDouble("sumb"), 0.001);
    assertFalse(rs.next());
  }

  @Test
  public void testTempTables() throws Exception {
    BSONTable bst = BSONTable.create(ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 1.0, "c", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "c", "bar"))));

    String[] queries = new String[] {
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a",
        "select sumb + 1 as added from last"
    };

    helper.initialize(ImmutableList.<Table>of(bst), queries);
    Statement stmt = helper.newStatement();
    ResultSet rs = helper.execute(stmt);
    assertTrue(rs.next());
    assertEquals(2.0, rs.getDouble("added"), 0.001);
    assertFalse(rs.next());
    stmt.close();

    bst.updateValues(ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1728, "b", 2.0, "c", "foo"))));
    stmt = helper.newStatement();
    rs = helper.execute(stmt);
    assertTrue(rs.next());
    assertEquals(3.0, rs.getDouble("added"), 0.001);
  }

  @Test
  public void testColumnMappings() throws Exception {
    BSONTable bst = BSONTable.create(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.<Object>of(Integer.class, Double.class, String.class),
        ImmutableMap.of("c", "d"));
    List<BasicDBObject> records = ImmutableList.of(
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 1.0, "d", "foo")),
        new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0, "d", "bar"))
    );
    bst.updateValues(records);

    String[] queries = new String[] {
        "select a, sum(b) as sumb from t1 where c = 'foo' group by a",
    };

    helper.initialize(ImmutableList.<Table>of(bst), queries);
    Statement stmt = helper.newStatement();
    ResultSet rs = helper.execute(stmt);
    assertTrue(rs.next());
    assertEquals(1729, rs.getInt("a"));
    assertEquals(1.0, rs.getDouble("sumb"), 0.001);
    assertFalse(rs.next());
  }
}
