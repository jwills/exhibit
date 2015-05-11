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

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.cloudera.exhibit.etl.config.AggConfig;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.Map;

public class TblCache {

  private final Map<GenericData.Record, Tbl> cache;
  private final AggConfig config;
  private final int aggIdx;
  private final Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter;
  private final SchemaProvider provider;

  public TblCache(AggConfig config, final int aggIdx,
                  final Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter,
                  final SchemaProvider provider) {
    this.cache = Maps.newHashMap();
    this.config = config;
    this.aggIdx = aggIdx;
    this.emitter = emitter;
    this.provider = provider;
  }

  public void update(GenericData.Record key, Obs obs) {
    Tbl tbl = cache.get(key);
    if (tbl == null) {
      if (cache.size() > config.cacheSize) {
        flush();
      }
      tbl = config.createTbl();
      tbl.initialize(provider);
      cache.put(key, tbl);
    }
    tbl.add(obs);
  }

  public void flush() {
    for (Map.Entry<GenericData.Record, Tbl> e : cache.entrySet()) {
      Tbl tbl = e.getValue();
      emitter.emit(Pair.of(e.getKey(), Pair.of(aggIdx, tbl.getValue())));
    }
    cache.clear();
  }
}
