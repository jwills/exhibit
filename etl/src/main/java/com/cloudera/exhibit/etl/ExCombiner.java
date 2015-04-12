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

import org.apache.avro.generic.GenericData;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;

import java.util.List;

public class ExCombiner extends CombineFn<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> {

  private SchemaProvider provider;
  private List<OutputConfig> configs;
  private PType<GenericData.Record> keyType;
  private PType<GenericData.Record> valueType;

  public ExCombiner(SchemaProvider provider, PType<GenericData.Record> keyType,
                    PType<GenericData.Record> valueType,
                    List<OutputConfig> configs) {
    this.provider = provider;
    this.configs = configs;
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public void initialize() {
    this.keyType.initialize(getConfiguration());
    this.valueType.initialize(getConfiguration());
  }

  @Override
  public void process(Pair<Pair<GenericData.Record, Integer>, Iterable<Pair<Integer, GenericData.Record>>> input,
                      Emitter<Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>>> emitter) {
    GenericData.Record key = keyType.getDetachedValue(input.first().first());
    int outIdx = (Integer) key.get("index");
    AggConfig ac = null;
    int aggIdx = -1;
    GenericData.Record merged = null;
    for (Pair<Integer, GenericData.Record> p : input.second()) {
      if (aggIdx < 0 || aggIdx != p.first()) {
        if (aggIdx >= 0) {
          increment("Exhibit", "MergedValues");
          GenericData.Record outValue = new GenericData.Record(provider.get(1));
          outValue.put("value", merged);
          emitter.emit(Pair.of(Pair.of(key, aggIdx), Pair.of(aggIdx, outValue)));
        }
        aggIdx = p.first();
        ac = configs.get(outIdx).aggregates.get(aggIdx);
        merged = null;
      }
      GenericData.Record val = valueType.getDetachedValue(p.second());
      merged = ac.merge(merged, (GenericData.Record) val.get("value"));
    }
    if (aggIdx >= 0) {
      increment("Exhibit", "MergedValues");
      GenericData.Record outValue = new GenericData.Record(provider.get(1));
      outValue.put("value", merged);
      emitter.emit(Pair.of(Pair.of(key, aggIdx), Pair.of(aggIdx, outValue)));
    }
  }
}
