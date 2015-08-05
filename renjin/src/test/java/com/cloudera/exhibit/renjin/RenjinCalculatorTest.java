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
package com.cloudera.exhibit.renjin;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import org.junit.Test;

public class RenjinCalculatorTest {
  @Test
  public void testBasic() throws Exception {
    RenjinCalculator rc = new RenjinCalculator("list(a=sum(a$x), s=search())");
    SimpleFrame sf = SimpleFrame.of(SimpleObs.of(SimpleObsDescriptor.of("x", ObsDescriptor.FieldType.INTEGER), 1729));
    Exhibit e = SimpleExhibit.of("a", sf);
    rc.initialize(e.descriptor());
    Iterable<Obs> obs = rc.apply(e);
    for (Obs o : obs) {
      System.out.println(o);
    }
    rc.cleanup();
  }
}
