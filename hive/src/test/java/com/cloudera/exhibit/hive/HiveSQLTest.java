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
package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.javascript.JSCalculator;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class HiveSQLTest {

  @Test
  public void testArrayOfStrings() {
    List<String> values = Lists.newArrayList("abc", "123", "xyz");
    HiveFrame hf = new HiveFrame(ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    hf.updateValues(values.toArray());
    SimpleExhibit ex = new SimpleExhibit(Obs.EMPTY, ImmutableMap.<String, Frame>of("T1", hf));
    SQLCalculator c = new SQLCalculator(new String[] { "SELECT count(*) from t1" });
    c.initialize(ex.descriptor());
    Obs first = Iterables.getOnlyElement(c.apply(ex));
    assertEquals(3L, first.get(0));
  }

  @Test
  public void testArrayOfStringsAsVec() {
    List<String> values = Lists.newArrayList("abc", "123", "xyz");
    Vec hf = new HiveVector(FieldType.STRING,
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    ((HiveVector) hf).updateValues(values.toArray());
    SimpleExhibit ex = new SimpleExhibit(Obs.EMPTY, ImmutableMap.<String, Frame>of(), ImmutableMap.of("t1", hf));
    SQLCalculator c = new SQLCalculator(new String[] { "SELECT count(*) from t1" });
    c.initialize(ex.descriptor());
    Obs first = Iterables.getOnlyElement(c.apply(ex));
    assertEquals(3L, first.get(0));
  }

  @Test
  public void testArrayOfInts() {
    List<Integer> values = Lists.newArrayList(1, 2, 3, 4, 5);
    HiveFrame hf = new HiveFrame(ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaIntObjectInspector));
    hf.updateValues(values.toArray());
    SimpleExhibit ex = new SimpleExhibit(Obs.EMPTY, ImmutableMap.<String, Frame>of("T1", hf));
    SQLCalculator c = new SQLCalculator(new String[] { "SELECT sum(c1) from t1" });
    c.initialize(ex.descriptor());
    Obs first = Iterables.getOnlyElement(c.apply(ex));
    assertEquals(15, first.get(0));
  }

  @Test
  public void testArrayOfIntsJS() {
    List<Integer> values = Lists.newArrayList(1, 2, 3, 4, 5);
    HiveFrame hf = new HiveFrame(ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaIntObjectInspector));
    hf.updateValues(values.toArray());
    SimpleExhibit ex = new SimpleExhibit(Obs.EMPTY, ImmutableMap.<String, Frame>of("T1", hf));
    JSCalculator jsc = new JSCalculator("T1[0]");
    jsc.initialize(ex.descriptor());
    Obs first = Iterables.getOnlyElement(jsc.apply(ex));
    assertEquals(1, first.get(0));
  }

  @Test
  public void testArrayOfIntsJSVec() {
    List<Integer> values = Lists.newArrayList(1, 2, 3, 4, 5);
    HiveVector hf = new HiveVector(FieldType.INTEGER,
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaIntObjectInspector));
    hf.updateValues(values.toArray());
    SimpleExhibit ex = new SimpleExhibit(Obs.EMPTY, ImmutableMap.<String, Frame>of(), ImmutableMap.<String, Vec>of("t1", hf));
    JSCalculator jsc = new JSCalculator("t1[0]");
    jsc.initialize(ex.descriptor());
    Obs first = Iterables.getOnlyElement(jsc.apply(ex));
    assertEquals(1, first.get(0));
  }
}
