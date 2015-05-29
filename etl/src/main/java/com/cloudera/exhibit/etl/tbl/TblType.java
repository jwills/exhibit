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
package com.cloudera.exhibit.etl.tbl;

import java.util.Map;

public enum TblType {
  LIST {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new ArrayTbl(false, options.get("name").toString(), values);
    }
  },
  SET {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new ArrayTbl(true, options.get("name").toString(), values);
    }
  },
  SUM {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new SumTbl(values);
    }
  },
  SUM_TOP {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new SumTopTbl(values, options);
    }
  },
  AVG {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new AvgTbl(values);
    }
  },
  PERCENTILE {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new PercentileTbl(values, options);
    }
  },
  TOP_LIST {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new TopListTbl(values, options);
    }
  },
  MIN {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new MinMaxTbl(true, values);
    }
  },
  MAX {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new MinMaxTbl(false, values);
    }
  }
  ;

  public abstract Tbl create(Map<String, String> values, Map<String, Object> options);
}
