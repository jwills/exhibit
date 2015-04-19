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
package com.cloudera.exhibit.etl.config;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.tbl.SumTbl;
import com.cloudera.exhibit.etl.tbl.Tbl;
import com.cloudera.exhibit.etl.tbl.TblType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AggConfig implements Serializable {

  public TblType type = TblType.SUM;
  public Map<String, Object> options = Maps.newHashMap();
  public FrameConfig frame = null;
  public List<String> keys = Lists.newArrayList();
  public Map<String, String> values = Maps.newHashMap();
  public long cacheSize = 5000;

  public Calculator getCalculator() {
    if (frame == null) {
      throw new IllegalStateException("Invalid AggConfig: no frame specified");
    }
    return frame.getCalculator();
  }

  public ObsDescriptor getFrameDescriptor(ExhibitDescriptor ed) {
    if (frame != null) {
      Calculator c = frame.getCalculator();
      return c.initialize(ed);
    }
    throw new IllegalStateException("Invalid AggConfig: no frame specified");
  }

  public Tbl createTbl() {
    //TODO: force validate values before this point
    return type.create(values, options);
  }
}
