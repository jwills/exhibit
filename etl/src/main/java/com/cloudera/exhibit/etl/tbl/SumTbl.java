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
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.cloudera.exhibit.etl.SchemaUtil.unwrapNull;

public class SumTbl implements Tbl {

  public static Object add(Object cur, Object next, Schema schema) {
    if (cur == null) {
      if (next == null) {
        schema = unwrapNull(schema);
        switch (schema.getType()) {
          case INT:
            return 0;
          case DOUBLE:
            return 0.0;
          case FLOAT:
            return 0.0f;
          case LONG:
            return 0L;
          case RECORD:
            return new GenericData.Record(schema);
          default:
            throw new UnsupportedOperationException("Cannot handle zero-values for null records of type: " + schema);
        }
      } else {
        return next;
      }
    } else if (next != null) {
      schema = unwrapNull(schema);
      switch (schema.getType()) {
        case INT:
          return ((Integer) cur) + ((Integer) next);
        case DOUBLE:
          return ((Double) cur) + ((Double) next);
        case FLOAT:
          return ((Float) cur) + ((Float) next);
        case LONG:
          return ((Long) cur) + ((Long) next);
        case RECORD:
          IndexedRecord rc = (IndexedRecord) cur;
          IndexedRecord nc = (IndexedRecord) next;
          for (int i = 0; i < schema.getFields().size(); i++) {
            rc.put(i, add(rc.get(i), nc.get(i), schema.getFields().get(i).schema()));
          }
          return rc;
        default:
          throw new UnsupportedOperationException("Cannot sum non-numeric type: " + schema.toString(true));
      }
    } else {
      return cur;
    }
  }

  private String[] inputFields;
  private String[] outputFields;
  private Sum[] sums;
  private Schema schema;

  public SumTbl(Map<String, String> values) {
    this.inputFields = new String[values.size()];
    this.outputFields = new String[values.size()];
    this.sums = new Sum[values.size()];
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
      switch (f.type) {
        case INTEGER:
          sums[i] = new IntSum();
          break;
        case LONG:
          sums[i] = new LongSum();
          break;
        case FLOAT:
          sums[i] = new FloatSum();
          break;
        case DOUBLE:
          sums[i] = new DoubleSum();
          break;
        default:
          throw new UnsupportedOperationException("Cannot sum field: " + f);
      }
    }
    this.schema = Schema.createRecord("ExValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    schema.setFields(fields);
    return new SchemaProvider(ImmutableList.of(schema, schema));
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.schema = provider.get(0);
    for (int i = 0; i < outputFields.length; i++) {
      switch (unwrapNull(schema.getField(outputFields[i]).schema()).getType()) {
        case INT:
          sums[i] = new IntSum();
          break;
        case LONG:
          sums[i] = new LongSum();
          break;
        case FLOAT:
          sums[i] = new FloatSum();
          break;
        case DOUBLE:
          sums[i] = new DoubleSum();
          break;
      }
    }
  }

  @Override
  public void add(Obs obs) {
    for (int i = 0; i < inputFields.length; i++) {
      sums[i].add((Number) obs.get(inputFields[i]));
    }
  }

  @Override
  public GenericData.Record getValue() {
    GenericData.Record value = new GenericData.Record(schema);
    for (int i = 0; i < outputFields.length; i++) {
      value.put(outputFields[i], sums[i].getValue());
    }
    return value;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    return (GenericData.Record) add(current, next, next.getSchema());
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record value) {
    return ImmutableList.of(value);
  }

  @Override
  public String toString() {
    return "SumTbl(" + Arrays.asList(inputFields) + ")";
  }

  public static interface Sum extends Serializable {
    void add(Number value);
    Number getValue();
  }

  public static class IntSum implements Sum {
    private int value = 0;

    @Override
    public void add(Number v) {
      value += v == null ? 0 : v.intValue();
    }

    @Override
    public Number getValue() {
      return value;
    }
  }

  public static class LongSum implements Sum {
    private long value = 0;

    @Override
    public void add(Number v) {
      value += v == null ? 0 : v.longValue();
    }

    @Override
    public Number getValue() {
      return value;
    }
  }

  public static class FloatSum implements Sum {
    private float value = 0;

    @Override
    public void add(Number v) {
      value += v == null ? 0 : v.floatValue();
    }

    @Override
    public Number getValue() {
      return value;
    }
  }

  public static class DoubleSum implements Sum {
    private double value = 0;

    @Override
    public void add(Number v) {
      value += v == null ? 0 : v.doubleValue();
    }

    @Override
    public Number getValue() {
      return value;
    }
  }
}
