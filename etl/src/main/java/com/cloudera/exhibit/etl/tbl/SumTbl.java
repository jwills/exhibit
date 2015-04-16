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

import java.util.List;
import java.util.Map;

import static com.cloudera.exhibit.etl.SchemaUtil.unwrapNull;

public class SumTbl implements Tbl {

  static Object add(Object cur, Object next, Schema schema) {
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
          default:
            throw new UnsupportedOperationException("Cannot handle zero-values for null records");
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

  private Map<String, String> values;
  private Schema schema;
  private GenericData.Record value;

  public SumTbl(Map<String, String> values) {
    this.values = values;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Map.Entry<String, String> e : values.entrySet()) {
      ObsDescriptor.Field f = od.get(od.indexOf(e.getKey()));
      fields.add(new Schema.Field(e.getValue(), AvroExhibit.getSchema(f.type), "", null));
    }
    this.schema = Schema.createRecord("ExValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    schema.setFields(fields);
    return new SchemaProvider(ImmutableList.of(schema, schema));
  }


  @Override
  public void initialize(SchemaProvider provider) {
    this.schema = provider.get(0);
  }

  @Override
  public void add(Obs obs) {
    GenericData.Record cur = new GenericData.Record(schema);
    for (Map.Entry<String, String> e : values.entrySet()) {
      cur.put(e.getValue(), obs.get(e.getKey()));
    }
    value = (GenericData.Record) add(cur, value, schema);
  }

  @Override
  public GenericData.Record getValue() {
    return value;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    return (GenericData.Record) add(current, next, next.getSchema());
  }

  @Override
  public GenericData.Record finalize(GenericData.Record value) {
    return value;
  }
}
