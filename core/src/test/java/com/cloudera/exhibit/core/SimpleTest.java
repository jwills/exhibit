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

import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleTest {

  public static final ObsDescriptor ATTR_DESC = SimpleObsDescriptor.builder()
      .booleanField("a")
      .stringField("b")
      .intField("c")
      .build();

  @Test
  public void testSimpleObsDescriptor() throws Exception{
    assertEquals(3, ATTR_DESC.size());
    assertEquals(1, ATTR_DESC.indexOf("b"));
    assertEquals(-1, ATTR_DESC.indexOf("q"));
    assertEquals(new ObsDescriptor.Field("c", FieldType.INTEGER), ATTR_DESC.get(2));
  }
}
