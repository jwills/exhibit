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

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class TopListTbl implements Tbl {

  private final Map<String, String> values;
  private final List<String> orderFields;
  private final List<Boolean> desc;
  private final int limit;

  private Schema inter;
  private Schema output;
  private PriorityQueue<GenericData.Record> list;

  public TopListTbl(Map<String, String> values, Map<String, Object> options) {
    this.values = values;
    if (options.get("order") == null) {
      throw new IllegalArgumentException("TOP_LIST aggregation must have a 'order' list of strings in its options");
    }
    this.orderFields = Lists.newArrayList();
    for (Object o : (List) options.get("order")) {
      orderFields.add(o.toString());
    }
    if (options.get("limit") == null) {
      throw new IllegalArgumentException("TOP_LIST aggregation must have a 'limit' integer value in its options");
    }
    this.limit = Integer.valueOf(options.get("limit").toString());
    if (limit <= 0) {
      throw new IllegalArgumentException("TOP_LIST aggregation must have a positive vale for the 'limit' option");
    }
    this.desc = Lists.newArrayList();
    if (options.get("desc") != null) {
      for (Object o : (List) options.get("desc")) {
        desc.add(Boolean.valueOf(o.toString()));
      }
    } else {
      for (String f : orderFields) {
        desc.add(Boolean.FALSE);
      }
    }
  }

  @Override
  public int arity() {
    return limit;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Map.Entry<String, String> e : values.entrySet()) {
      ObsDescriptor.Field f = od.get(od.indexOf(e.getKey()));
      fields.add(new Schema.Field(e.getValue(), AvroExhibit.getSchema(f.type), "", null));
    }
    Schema output = Schema.createRecord("ExTopList_" + outputId + "_" + aggIdx, "", "exhibit", false);
    output.setFields(fields);

    Schema inter = Schema.createRecord("ExTopListInter_" + outputId + "_" + aggIdx, "", "exhibit", false);
    inter.setFields(Lists.newArrayList(new Schema.Field("list", Schema.createArray(output), "", null)));
    return new SchemaProvider(ImmutableList.of(inter, output));
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.inter = provider.get(0);
    this.output = provider.get(1);
    this.list = new PriorityQueue<GenericData.Record>(limit, new Comparator<GenericData.Record>() {
      @Override
      public int compare(GenericData.Record o1, GenericData.Record o2) {
        for (int i = 0; i < orderFields.size(); i++) {
          Object v1 = o1.get(orderFields.get(i));
          Object v2 = o2.get(orderFields.get(i));
          if (v1 == null) {
            if (v2 != null) {
              return 1;
            }
          } else {
            int cmp = ((Comparable) v1).compareTo(v2);
            if (cmp != 0) {
              return desc.get(i).booleanValue() ? -cmp : cmp;
            }
          }
        }
        return 0;
      }
    });
  }

  @Override
  public void add(Obs obs) {
    GenericData.Record rec = new GenericData.Record(output);
    for (Map.Entry<String, String> e : values.entrySet()) {
      rec.put(e.getValue(), obs.get(e.getKey()));
    }
    update(rec);
  }

  private void update(GenericData.Record v) {
    list.add(v);
    while (list.size() > limit) {
      list.poll();
    }
  }
  @Override
  public GenericData.Record getValue() {
    GenericData.Record ret = new GenericData.Record(inter);
    List<GenericData.Record> out = Lists.newArrayList(list);
    ret.put(0, out);
    return ret;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    if (current == null) {
      return next;
    }
    list.clear();
    List curList = (List) current.get(0);
    if (curList != null) {
      for (Object o : curList) {
        update((GenericData.Record) o);
      }
    }
    List nextList = (List) next.get(0);
    if (nextList != null) {
      for (Object o : nextList) {
        update((GenericData.Record) o);
      }
    }
    return getValue();
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record value) {
    return (List<GenericData.Record>) value.get(0);
  }
}
