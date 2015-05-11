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

import com.cloudera.exhibit.etl.tbl.Tbl;
import com.cloudera.exhibit.etl.tbl.TblType;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ComponentConfig implements Serializable {
  public String source;
  public List<String> keys;
  public boolean embedded = false;
  public int cacheSize = 5000;

  public TblType type;
  public Map<String, Object> options = Maps.newHashMap();
  public Object values;

  public Tbl createTbl() {
    if (type == null) {
      return null;
    }
    return type.create(getValues(), options);
  }

  public Map<String, String> getValues() {
    Map<String, String> ret = Maps.newHashMap();
    if (values instanceof List) {
      for (Object o : (List) values) {
        String str = o.toString();
        ret.put(str, str);
      }
    } else if (values instanceof Map) {
      for (Map.Entry<?, ?> e : ((Map<?, ?>) values).entrySet()) {
        ret.put(e.getKey().toString(), e.getValue().toString());
      }
    } else {
      throw new IllegalStateException("Unsupported values type: " + values + " must be list or map");
    }
    return ret;
  }
}
