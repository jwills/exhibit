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

//TODO: maybe ExtentTbl, with an option to track both min and max?
public class MinMaxTbl implements Tbl {

  private final boolean min;
  private final String[] inputFields;
  private final String[] outputFields;
  private Schema schema;
  private GenericData.Record value;

  public MinMaxTbl(boolean min, Map<String, String> values) {
    this.min = min;
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
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < inputFields.length; i++) {
      ObsDescriptor.Field f = od.get(od.indexOf(inputFields[i]));
      fields.add(new Schema.Field(outputFields[i], AvroExhibit.getSchema(f.type), "", null));
    }
    this.schema = Schema.createRecord("ExMinMaxValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    schema.setFields(fields);
    return new SchemaProvider(ImmutableList.of(schema, schema));
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.schema = provider.get(0);
    this.value = new GenericData.Record(schema);
  }

  @Override
  public void add(Obs obs) {
    for (int i = 0; i < inputFields.length; i++) {
      Object res = obs.get(inputFields[i]);
      if (res != null) {
        Object cur = value.get(i);
        if (cur == null) {
          value.put(i, res);
        } else {
          int cmp = ((Comparable) cur).compareTo(res);
          if ((cmp > 0 && min) || (cmp < 0 && !min)) {
            value.put(i, res);
          }
        }
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
    for (int i = 0; i < schema.getFields().size(); i++) {
      Object cur = current.get(i);
      Object nex = next.get(i);
      if (cur == null) {
        current.put(i, nex);
      } else if (nex != null) {
        int cmp = ((Comparable) cur).compareTo(nex);
        if ((cmp > 0 && min) || (cmp < 0 && !min)) {
          current.put(i, nex);
        }
      }
    }
    return current;
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record value) {
    return ImmutableList.of(value);
  }
}
