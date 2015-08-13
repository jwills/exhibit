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
package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
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
}
