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
  private final List<String> json;
  private transient List<Schema> schemas;
  private int outputIndex;
  private transient GenericData.Record lastKey = null;
  private transient GenericData.Record lastValue = null;

  public MergeRowsFn(List<Schema> schemas) {
    this.json = Lists.newArrayList(Lists.transform(schemas, new Function<Schema, String>() {
      @Nullable
      @Override
      public String apply(Schema schema) {
        return schema.toString();
      }
    }));
  }

  @Override
  public void initialize() {
    final Schema.Parser sp = new Schema.Parser();
    this.schemas = Lists.newArrayList(Lists.transform(json, new Function<String, Schema>() {
      @Nullable
      @Override
      public Schema apply(@Nullable String s) {
        return sp.parse(s);
      }
    }));
    lastKey = null;
    lastValue = null;
  }
  @Override
  public void process(Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> input,
                      Emitter<Pair<Integer, GenericData.Record>> emitter) {
    if (lastKey == null || !lastKey.equals(input.first().first())) {
      if (lastKey != null) {
        emitter.emit(Pair.of(outputIndex, lastValue));
      }
      lastKey = input.first().first();
      outputIndex = (Integer) lastKey.get("index");
      lastValue = new GenericData.Record(schemas.get(outputIndex));
      GenericRecord innerKey = (GenericRecord) lastKey.get("key");
      for (Schema.Field sf : innerKey.getSchema().getFields()) {
        lastValue.put(sf.name(), innerKey.get(sf.name()));
      }
    }
    GenericData.Record value = input.second().second();
    for (Schema.Field sf : value.getSchema().getFields()) {
      lastValue.put(sf.name(), value.get(sf.name()));
    }
  }

  @Override
  public void cleanup(Emitter<Pair<Integer, GenericData.Record>> emitter) {
    if (lastKey != null) {
      emitter.emit(Pair.of(outputIndex, lastValue));
    }
  }
}
