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
package com.cloudera.exhibit.core;

import com.cloudera.exhibit.core.composite.CompositeObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompositeTest {

  public static final ObsDescriptor F1_DESC = SimpleObsDescriptor.builder()
      .doubleField("v0")
      .stringField("v1")
      .build();

  public static final ObsDescriptor F2_DESC = SimpleObsDescriptor.builder()
      .intField("x")
      .build();

  @Test
  public void testCompositeObsDescriptor() throws Exception {
    CompositeObsDescriptor cod = new CompositeObsDescriptor(ImmutableList.of(F1_DESC, F2_DESC));
    assertEquals(3, cod.size());
    assertEquals(1, cod.indexOf("v1"));
    assertEquals(2, cod.indexOf("x"));
    assertEquals(-1, cod.indexOf("v3"));
    assertEquals(new ObsDescriptor.Field("x", ObsDescriptor.FieldType.INTEGER), cod.get(2));
  }

  @Test
  public void testCompositeObsDescriptorWithEmpty() throws Exception {
    CompositeObsDescriptor cod = new CompositeObsDescriptor(ImmutableList.of(ObsDescriptor.EMPTY, F1_DESC));
    assertEquals(2, cod.size());
    assertEquals(0, cod.indexOf("v0"));
    assertEquals(1, cod.indexOf("v1"));
    assertEquals(-1, cod.indexOf("q"));
    assertEquals(new ObsDescriptor.Field("v1", ObsDescriptor.FieldType.STRING), cod.get(1));
    assertEquals(new ObsDescriptor.Field("v0", ObsDescriptor.FieldType.DOUBLE), cod.get(0));
  }
}
