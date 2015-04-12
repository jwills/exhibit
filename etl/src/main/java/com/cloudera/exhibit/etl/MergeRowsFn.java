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
package com.cloudera.exhibit.etl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import javax.annotation.Nullable;
import java.util.List;

public class MergeRowsFn extends DoFn<
    Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>>,
    Pair<Integer, GenericData.Record>> {
  private final String wrapperJson;
  private transient Schema wrapperSchema;
  private transient List<Schema> schemas;
  private transient Integer outputIndex;
  private transient GenericData.Record lastKey = null;
  private transient GenericData.Record lastValue = null;

  public MergeRowsFn(Schema unionSchema) {
    this.wrapperJson = unionSchema.toString();
  }

  @Override
  public void initialize() {
    final Schema.Parser sp = new Schema.Parser();
    this.wrapperSchema = sp.parse(wrapperJson);
    this.schemas = wrapperSchema.getField("value").schema().getTypes();
    for (Schema s : this.schemas) {
      System.out.println(s.toString(true));
    }
    lastKey = null;
    lastValue = null;
  }

  @Override
  public void process(Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> input,
                      Emitter<Pair<Integer, GenericData.Record>> emitter) {
    System.out.println("New key = " + input.first().first());
    if (lastKey == null || !lastKey.equals(input.first().first())) {
      if (lastKey != null) {
        System.out.println("Emitting value = " + lastValue + " to out = " + outputIndex);
        GenericData.Record wrapper = new GenericData.Record(wrapperSchema);
        wrapper.put("value", lastValue);
        increment("Exhibit", "ValueWritten" + outputIndex);
        emitter.emit(Pair.of(outputIndex, wrapper));
        lastKey = null;
        lastValue = null;
      }
      lastKey = input.first().first();
      System.out.println("New lastKey = " + lastKey);
      outputIndex = (Integer) lastKey.get("index");
      lastValue = new GenericData.Record(schemas.get(outputIndex));
      GenericRecord innerKey = (GenericRecord) lastKey.get("key");
      for (Schema.Field sf : innerKey.getSchema().getFields()) {
        lastValue.put(sf.name(), innerKey.get(sf.name()));
      }
      System.out.println("New last value = " + lastValue);
    }
    GenericData.Record value = (GenericData.Record) input.second().second().get("value");
    System.out.println("New value = " + value);
    for (Schema.Field sf : value.getSchema().getFields()) {
      lastValue.put(sf.name(), value.get(sf.name()));
    }
    System.out.println("Last value is now = " + lastValue);
  }

  @Override
  public void cleanup(Emitter<Pair<Integer, GenericData.Record>> emitter) {
    if (lastKey != null && lastValue != null) {
      System.out.println("Writing out last key = " + lastKey);
      System.out.println("With value = " + lastValue);
      GenericData.Record wrapper = new GenericData.Record(wrapperSchema);
      wrapper.put("value", lastValue);
      increment("Exhibit", "ValueWritten" + outputIndex);
      emitter.emit(Pair.of((Integer) lastKey.get("index"), wrapper));
    }
    lastKey = null;
    lastValue = null;
  }
}
