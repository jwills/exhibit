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
package com.cloudera.exhibit.etl.config;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.tbl.Tbl;
import com.cloudera.exhibit.etl.tbl.TblType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AggConfig implements Serializable {

  // The type of aggregation that will be performed on the computed records (SUM, PERCENTILE, SUM_TOP, TOP)
  public TblType type = TblType.SUM;

  // Table-specific options (e.g., which fields to sort on for the TOP tbl)
  public Map<String, Object> options = Maps.newHashMap();

  // The frame used to generate the output keys and values
  public FrameConfig frame = null;

  // The names of the grouping keys for the computed frame if they are different from the
  // names of the keys for the parent {@link OutputConfig}.
  public List<String> keys = Lists.newArrayList();

  // A mapping from the names of fields in the computed frame to their names in the output
  // aggregation (to allow for columns to be renamed)
  public Map<String, String> values = Maps.newHashMap();

  // The maximum number of keys whose aggregate values should be cached in memory.
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
