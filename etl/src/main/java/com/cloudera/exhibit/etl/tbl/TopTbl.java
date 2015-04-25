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

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;

import java.util.List;
import java.util.Map;

public class TopTbl implements Tbl {

  private final Map<String, String> values;
  private final List<String> orderFields;
  private final List<Boolean> desc;
  private final int limit;

  public TopTbl(Map<String, String> values, Map<String, Object> options) {
    this.values = values;
    if (options.get("by") == null) {
      throw new IllegalArgumentException("TOP aggregation must have a 'by' list of strings in its options");
    }
    this.orderFields = Lists.newArrayList();
    for (Object o : (List) options.get("by")) {
      orderFields.add(o.toString());
    }
    if (options.get("limit") == null) {
      throw new IllegalArgumentException("TOP aggregation must have a 'limit' integer value in its options");
    }
    this.limit = Integer.valueOf(options.get("limit").toString());
    if (options.get("desc") != null) {
      this.desc = Lists.newArrayList();
      for (Object o : (List) options.get("desc")) {
        desc.add(Boolean.valueOf(o.toString()));
      }
    } else {
      this.desc = Lists.newArrayList(Lists.transform(orderFields, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          return Boolean.FALSE;
        }
      }));
    }
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    return null;
  }

  @Override
  public void initialize(SchemaProvider provider) {

  }

  @Override
  public void add(Obs obs) {

  }

  @Override
  public GenericData.Record getValue() {
    return null;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    return null;
  }

  @Override
  public GenericData.Record finalize(GenericData.Record value) {
    return null;
  }
}
