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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SumTopTbl implements Tbl {

  private final Map<String, String> values;
  private final String subKey;
  private final String orderKey;
  private final int limit;

  private Schema intermediate;
  private Schema output;
  private GenericData.Record wrapper;

  public SumTopTbl(Map<String, String> values, Map<String, Object> options) {
    this.values = values;
    if (options.get("by") == null) {
      throw new IllegalArgumentException("SUM_TOP aggregation must have a 'by' key in its options");
    }
    this.subKey = options.get("by").toString();
    if (options.get("order") == null) {
      throw new IllegalArgumentException("SUM_TOP aggregation must have an 'order' key in its options");
    }
    this.orderKey = options.get("order").toString();
    if (options.get("limit") == null) {
      throw new IllegalArgumentException("SUM_TOP aggregation must have a 'limit' integer value in its options");
    }
    this.limit = Integer.valueOf(options.get("limit").toString());
    Preconditions.checkArgument(limit > 0, "limit option must be greater than zero, found: " + limit);
  }

  @Override
  public int arity() {
    return 1;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    // Validate subKey and ordering args
    int subKeyIdx = od.indexOf(subKey);
    if (subKeyIdx < 0) {
      throw new IllegalArgumentException(String.format("SUM_TOP by key named '%s' not found in query",
          subKey));
    }
    ObsDescriptor.Field subKeyField = od.get(subKeyIdx);
    if (subKeyField == null || subKeyField.type != ObsDescriptor.FieldType.STRING) {
      throw new IllegalArgumentException(String.format("SUM_TOP by key named '%s' must be of type string, found %s",
          subKey, subKeyField == null ? "null" : subKeyField.type));
    }
    ObsDescriptor.Field orderField = od.get(od.indexOf(orderKey));
    if (orderField == null) {
      throw new IllegalArgumentException("SUM_TOP missing ordering field from input frame: " + orderKey);
    }
    List<Schema.Field> interFields = Lists.newArrayList();
    for (Map.Entry<String, String> e : values.entrySet()) {
      if (!subKey.equals(e.getKey())) {
        ObsDescriptor.Field f = od.get(od.indexOf(e.getKey()));
        interFields.add(new Schema.Field(e.getValue(), AvroExhibit.getSchema(f.type), "", null));
      }
    }
    Schema interValue = Schema.createRecord("ExSumTopInterValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    interValue.setFields(interFields);
    this.intermediate = Schema.createRecord("ExSumTopInter_" + outputId + "_" + aggIdx, "", "exhibit", false);
    this.intermediate.setFields(Lists.newArrayList(new Schema.Field("value", Schema.createMap(interValue), "", null)));

    List<Schema.Field> outputFields = Lists.newArrayList();
    for (int i = 1; i <= limit; i++) {
      for (Map.Entry<String, String> e : values.entrySet()) {
        ObsDescriptor.Field f = od.get(od.indexOf(e.getKey()));
        String fieldName = outputFieldName(e.getValue(), i);
        outputFields.add(new Schema.Field(fieldName, AvroExhibit.getSchema(f.type), "", null));
      }
    }
    this.output = Schema.createRecord("ExSumTopOutput_" + outputId + "_" + aggIdx, "", "exhibit", false);
    output.setFields(outputFields);
    return new SchemaProvider(ImmutableList.of(intermediate, output));
  }

  private static String outputFieldName(String name, int index) {
    return String.format("%s_n%d", name, index);
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.intermediate = provider.get(0);
    this.output = provider.get(1);
    this.wrapper = new GenericData.Record(intermediate);
    this.wrapper.put("value", Maps.newHashMap());
  }

  @Override
  public void add(Obs obs) {
    Object subKeyValue = obs.get(subKey);
    if (subKeyValue != null) {
      String skv = subKeyValue.toString();
      Map<CharSequence, GenericData.Record> inner = (Map<CharSequence, GenericData.Record>) wrapper.get("value");
      Schema vschema = intermediate.getField("value").schema().getValueType();
      GenericData.Record innerValue = new GenericData.Record(vschema);
      for (Map.Entry<String, String> e : values.entrySet()) {
        if (!subKey.equals(e.getKey())) {
          innerValue.put(e.getValue(), obs.get(e.getKey()));
        }
      }
      GenericData.Record sum = (GenericData.Record) SumTbl.add(inner.get(skv), innerValue, vschema);
      inner.put(skv, sum);
    }
  }

  @Override
  public GenericData.Record getValue() {
    return wrapper;
  }

  @Override
  public GenericData.Record merge(
      GenericData.Record current,
      GenericData.Record next) {
    if (current == null) {
      return next;
    }
    Map<CharSequence, GenericData.Record> curValue = (Map<CharSequence, GenericData.Record>) current.get("value");
    Map<CharSequence, GenericData.Record> nextValue = (Map<CharSequence, GenericData.Record>) next.get("value");
    Schema vschema = intermediate.getField("value").schema().getValueType();
    GenericData.Record merged = new GenericData.Record(vschema);
    Map<CharSequence, GenericData.Record> mergedValue = (Map<CharSequence, GenericData.Record>) merged.get("value");
    for (CharSequence key : Sets.union(curValue.keySet(), nextValue.keySet())) {
      GenericData.Record sum = (GenericData.Record) SumTbl.add(curValue.get(key), nextValue.get(key), vschema);
      mergedValue.put(key, sum);
    }
    return merged;
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record input) {
    Map<CharSequence, GenericData.Record> curValue = (Map<CharSequence, GenericData.Record>) input.get("value");
    List<Map.Entry<CharSequence, GenericData.Record>> elements = Lists.newArrayList(curValue.entrySet());
    Collections.sort(elements, new SumTopComparator(orderKey));
    GenericData.Record res = new GenericData.Record(output);
    for (int i = 1; i <= limit && i <= elements.size(); i++) {
      Map.Entry<CharSequence, GenericData.Record> cur = elements.get(i - 1);
      res.put(outputFieldName(subKey, i), cur.getKey());
      for (Map.Entry<String, String> e : values.entrySet()) {
        if (!subKey.equals(e.getKey())) {
          res.put(outputFieldName(e.getValue(), i), cur.getValue().get(e.getKey()));
        }
      }
    }
    return ImmutableList.of(res);
  }

  private static class SumTopComparator implements Comparator<Map.Entry<CharSequence, GenericData.Record>> {

    private final String orderField;

    public SumTopComparator(String orderField) {
      this.orderField = orderField;
    }

    @Override
    public int compare(Map.Entry<CharSequence, GenericData.Record> o1, Map.Entry<CharSequence, GenericData.Record> o2) {
      Object k1 = o1.getValue().get(orderField);
      Object k2 = o2.getValue().get(orderField);
      return -((Comparable) k1).compareTo(k2);
    }
  }
}
