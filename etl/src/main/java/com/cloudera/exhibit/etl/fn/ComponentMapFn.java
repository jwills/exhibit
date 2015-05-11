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
package com.cloudera.exhibit.etl.fn;

import com.cloudera.exhibit.avro.AvroObs;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.cloudera.exhibit.etl.config.ComponentConfig;
import com.cloudera.exhibit.etl.tbl.Tbl;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.LinkedHashMap;
import java.util.Map;

public class ComponentMapFn extends DoFn<GenericData.Record,
    Pair<GenericData.Record,
         Pair<Integer, GenericData.Record>>> {

  private final int componentId;
  private final String keyJson;
  private final ComponentConfig config;
  private final SchemaProvider provider;

  private transient Schema keySchema;
  private transient LinkedHashMap<GenericData.Record, Tbl> cache;

  public ComponentMapFn(
      int componentId,
      ComponentConfig config,
      Schema keySchema,
      SchemaProvider provider) {
    this.componentId = componentId;
    this.keyJson = keySchema.toString();
    this.config = config;
    this.provider = provider;
  }

  @Override
  public void initialize() {
    this.keySchema = (new Schema.Parser()).parse(keyJson);
    this.cache = Maps.newLinkedHashMap();
  }

  @Override
  public void process(GenericData.Record record,
      Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter) {
    GenericData.Record key = new GenericData.Record(keySchema);
    for (int i = 0; i < config.keys.size(); i++) {
      Object k = record.get(config.keys.get(i));
      key.put(i, k);
    }
    //TODO: filter invalid keys
    if (config.embedded) {
      GenericData.Record value = new GenericData.Record(provider.get(0));
      for (Map.Entry<String, String> e : config.getValues().entrySet()) {
        value.put(e.getValue(), record.get(e.getKey()));
      }
      emitter.emit(Pair.of(key, Pair.of(componentId, value)));
    } else {
      Tbl tbl = cache.get(key);
      if (tbl == null) {
        tbl = config.createTbl();
        tbl.initialize(provider);
        cache.put(key, tbl);
        if (cache.size() > config.cacheSize) {
          cleanup(emitter);
        }
      }
      tbl.add(new AvroObs(record));
    }
  }

  @Override
  public void cleanup(
          Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter) {
    for (Map.Entry<GenericData.Record, Tbl> e : cache.entrySet()) {
      emitter.emit(Pair.of(e.getKey(), Pair.of(componentId, e.getValue().getValue())));
    }
    cache.clear();
  }
}
