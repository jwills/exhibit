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
package com.cloudera.exhibit.etl.fn;

import com.cloudera.exhibit.etl.SchemaProvider;
import com.cloudera.exhibit.etl.config.OutputConfig;
import com.cloudera.exhibit.etl.tbl.Tbl;
import com.cloudera.exhibit.etl.tbl.TblFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.List;

public class MergeRowsFn extends DoFn<
    Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>>,
    Pair<Integer, GenericData.Record>> {

  private final TblFactory tblFactory;
  private final String wrapperJson;

  private transient Schema wrapperSchema;
  private transient List<Schema> schemas;
  private transient List<List<Tbl>> tbls;
  private transient Integer outputIndex;
  private transient GenericData.Record lastKey = null;
  private transient GenericData.Record lastValue = null;

  public MergeRowsFn(TblFactory tblFactory, Schema unionSchema) {
    this.tblFactory = tblFactory;
    this.wrapperJson = unionSchema.toString();
  }

  @Override
  public void initialize() {
    final Schema.Parser sp = new Schema.Parser();
    this.wrapperSchema = sp.parse(wrapperJson);
    this.schemas = wrapperSchema.getField("value").schema().getTypes();
    lastKey = null;
    lastValue = null;
    this.tbls = tblFactory.createAll();
  }

  @Override
  public void process(Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> input,
                      Emitter<Pair<Integer, GenericData.Record>> emitter) {
    if (lastKey == null || !lastKey.equals(input.first().first())) {
      if (lastKey != null) {
        emit(emitter);
      }
      lastKey = input.first().first();
      outputIndex = (Integer) lastKey.get("index");
      lastValue = new GenericData.Record(schemas.get(outputIndex));
      GenericRecord innerKey = (GenericRecord) lastKey.get("key");
      for (Schema.Field sf : innerKey.getSchema().getFields()) {
        lastValue.put(sf.name(), innerKey.get(sf.name()));
      }
    }
    Pair<Integer, GenericData.Record> aggValue = input.second();
    int aggIdx = aggValue.first();
    List<GenericData.Record> values;
    Tbl tbl = tbls.get(outputIndex).get(aggIdx);
    if (tbl == null) {
      values = ImmutableList.of((GenericData.Record) aggValue.second().get("value"));
    } else {
      values = tbl.finalize((GenericData.Record) aggValue.second().get("value"));
    }
    if (values.size() == 1) {
      GenericData.Record value = values.get(0);
      for (Schema.Field sf : value.getSchema().getFields()) {
        lastValue.put(sf.name(), value.get(sf.name()));
      }
    } else if (tbls.get(outputIndex).size() == 1) {
      for (GenericData.Record value : values) {
        for (Schema.Field sf : value.getSchema().getFields()) {
          lastValue.put(sf.name(), value.get(sf.name()));
        }
        emit(emitter);
      }
    }
  }

  @Override
  public void cleanup(Emitter<Pair<Integer, GenericData.Record>> emitter) {
    if (lastKey != null && lastValue != null) {
      emit(emitter);
    }
    lastKey = null;
    lastValue = null;
  }

  private void emit(Emitter<Pair<Integer, GenericData.Record>> emitter) {
    GenericData.Record wrapper = new GenericData.Record(wrapperSchema);
    wrapper.put("value", lastValue);
    increment("Exhibit", "ValueWritten" + outputIndex);
    emitter.emit(Pair.of(outputIndex, wrapper));
  }
}
