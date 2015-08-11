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
package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.List;

public class VectorTest extends TestCase {

  public void testGetType() throws Exception {
    double [] doubles = new double[]{1.0, 2.0, 3.0};
    DoubleVector dv = new DoubleVector(doubles);
    assertEquals(FieldType.DOUBLE, dv.getType());

    List list = ImmutableList.of("A", "B", "C");
    Vector vector = VectorBuilder.build(FieldType.STRING, list);
    assertEquals(FieldType.STRING, vector.getType());
  }

  public void testInvalidDoubles() throws Exception {
    List list = ImmutableList.of("A", "B", "C");
    try {
      VectorBuilder.build(FieldType.DOUBLE, list);
    } catch ( IllegalArgumentException ex ){
      // success, exception should be thrown
      return;
    }
    fail("Invalid Doubles were created into a vector");
  }

  public void testGet() throws Exception {
    double [] doubles = new double[]{1.0, 2.0, 3.0};
    List doubleList = Doubles.asList(doubles);
    Vector dv = VectorBuilder.build(FieldType.DOUBLE, doubleList);
    assertEquals(2.0, dv.get(1));

    List list = ImmutableList.of("A", "B", "C");
    Vector vector = VectorBuilder.build(FieldType.STRING, list);
    assertEquals("C", vector.get(2));
  }

  public void testSize() throws Exception {
    double [] doubles = new double[]{1.0, 2.0, 3.0};
    DoubleVector dv = new DoubleVector(doubles);
    assertEquals(3, dv.size());
  }

  public void testIterator() throws Exception {
    double [] doubles = new double[]{1.0, 2.0, 3.0};
    DoubleVector dv = new DoubleVector(doubles);
    Iterator<Object> it = dv.iterator();
    assertTrue(it.hasNext());
    assertEquals(1.0, it.next());

    List list = ImmutableList.of("A", "B", "C");
    Vector vector = VectorBuilder.build(FieldType.STRING, list);
    Iterator<Object> vIt = vector.iterator();
    assertEquals("A", vIt.next());
  }
}