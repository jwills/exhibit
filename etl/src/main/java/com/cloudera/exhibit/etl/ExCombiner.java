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

import java.util.List;

public class ExCombiner extends CombineFn<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> {

  private List<OutputConfig> configs;

  public ExCombiner(List<OutputConfig> configs) {
    this.configs = configs;
  }

  @Override
  public void process(Pair<Pair<GenericData.Record, Integer>, Iterable<Pair<Integer, GenericData.Record>>> input,
                      Emitter<Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>>> emitter) {
    GenericData.Record key = input.first().first();
    int outIdx = (Integer) key.get("index");
    AggConfig ac = null;
    int aggIdx = -1;
    GenericData.Record merged = null;
    for (Pair<Integer, GenericData.Record> p : input.second()) {
      if (aggIdx < 0 || aggIdx != p.first()) {
        if (aggIdx >= 0) {
          emitter.emit(Pair.of(Pair.of(key, aggIdx), Pair.of(aggIdx, merged)));
        }
        aggIdx = p.first();
        ac = configs.get(outIdx).aggregates.get(aggIdx);
        merged = null;
      }
      merged = ac.merge(merged, p.second());
    }
    if (aggIdx >= 0) {
      emitter.emit(Pair.of(Pair.of(key, aggIdx), Pair.of(aggIdx, merged)));
    }

  }
}
