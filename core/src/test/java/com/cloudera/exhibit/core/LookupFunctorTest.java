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

import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibitDescriptor;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LookupFunctorTest {

  public static final ObsDescriptor ATTR_DESC = SimpleObsDescriptor.builder()
      .booleanField("a")
      .stringField("b")
      .intField("c")
      .build();

  public static final Vector VECTOR
      = VectorBuilder.doubles(ImmutableList.<Object>of(1.0, 2.0, 3.0));

  public static final ExhibitDescriptor EXHIBIT_DESCRIPTOR
      = new SimpleExhibitDescriptor(ATTR_DESC
      , ImmutableMap.of("df", ATTR_DESC)
      , ImmutableMap.of("vec", VECTOR.getType()));

  public static final Exhibit EXHIBIT = Exhibits.defaultValues(EXHIBIT_DESCRIPTOR);

  @Test
  public void testInitialize() throws Exception {
    LookupFunctor lf = new LookupFunctor("df");
    assertEquals(SimpleExhibitDescriptor.of("df", ATTR_DESC)
        , lf.initialize(EXHIBIT_DESCRIPTOR));
  }

  @Test
  public void testApply() throws Exception {
    Exhibit expected = SimpleExhibit.of("df", EXHIBIT.frames().get("df"));
    LookupFunctor lf = new LookupFunctor("df");
    assertEquals(expected, lf.apply(EXHIBIT));
  }

  @Test
  public void testNonExistentFrame() throws Exception {
    try{
      Exhibit expected = SimpleExhibit.of("df", EXHIBIT.frames().get("df"));
      LookupFunctor lf = new LookupFunctor("bad-name");
      lf.apply(expected);
      fail("Lookup with non-existent frame succeeded");
    } catch(Exception e ){
    }
  }
}