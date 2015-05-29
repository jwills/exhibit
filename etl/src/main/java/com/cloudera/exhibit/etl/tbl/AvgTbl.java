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

import java.util.List;
import java.util.Map;

public class AvgTbl implements Tbl {

  private String[] inputFields;
  private String[] outputFields;
  private SchemaProvider schemaProvider;
  private GenericData.Record value;

  public AvgTbl(Map<String, String> values) {
    this.inputFields = new String[values.size()];
    this.outputFields = new String[values.size()];
    int index = 0;
    for (Map.Entry<String, String> e : values.entrySet()) {
      inputFields[index] = e.getKey();
      outputFields[index] = e.getValue();
      index++;
    }
  }


  @Override
  public int arity() {
    return 1;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    List<Schema.Field> interFields = Lists.newArrayList();
    List<Schema.Field> outFields = Lists.newArrayList();
    Schema ds = AvroExhibit.getSchema(ObsDescriptor.FieldType.DOUBLE);
    for (int i = 0; i < inputFields.length; i++) {
      interFields.add(new Schema.Field(outputFields[i], ds, "", null));
      interFields.add(new Schema.Field(outputFields[i] + "_cnt", ds, "", null));
      outFields.add(new Schema.Field(outputFields[i], ds, "", null));
    }
    Schema inter = Schema.createRecord("ExInterAvgValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    inter.setFields(interFields);
    Schema outer = Schema.createRecord("ExAvgValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    outer.setFields(outFields);
    return new SchemaProvider(ImmutableList.of(inter, outer));
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.schemaProvider = provider;
    this.value = new GenericData.Record(schemaProvider.get(0));
    for (int i = 0; i < value.getSchema().getFields().size(); i++) {
      value.put(i, 0.0);
    }
  }

  @Override
  public void add(Obs obs) {
    for (int i = 0; i < inputFields.length; i++) {
      Number n = (Number) obs.get(inputFields[i]);
      if (n != null) {
        double cnt = (Double) value.get(outputFields[i] + "_cnt");
        double current = (Double) value.get(outputFields[i]);
        double next = current + (n.doubleValue() - current) / (cnt + 1.0);
        value.put(outputFields[i] + "_cnt", cnt + 1.0);
        value.put(outputFields[i], next);
      }
    }
  }

  @Override
  public GenericData.Record getValue() {
    return value;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    if (current == null) {
      return next;
    }
    for (int i = 0; i < outputFields.length; i++) {
      double avg1 = (Double) current.get(outputFields[i]);
      double cnt1 = (Double) current.get(outputFields[i] + "_cnt");
      double avg2 = (Double) next.get(outputFields[i]);
      double cnt2 = (Double) next.get(outputFields[i] + "_cnt");
      double merged = avg1 + (avg2 - avg1) * cnt2 / (cnt1 + cnt2);
      current.put(outputFields[i], merged);
      current.put(outputFields[i] + "_cnt", cnt1 + cnt2);
    }
    return current;
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record value) {
    GenericData.Record out = new GenericData.Record(schemaProvider.get(1));
    for (int i = 0; i < outputFields.length; i++) {
      out.put(outputFields[i], value.get(outputFields[i]));
    }
    return ImmutableList.of(out);
  }
}
