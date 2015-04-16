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

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.cloudera.exhibit.etl.config.AggConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public class TblCache {

  private Cache<GenericData.Record, Tbl> cache;

  public TblCache(final AggConfig config, final int aggIdx,
                  final Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter,
                  final SchemaProvider provider) {
    this.cache = CacheBuilder.<GenericData.Record, Tbl>from(config.cache)
        .removalListener(new RemovalListener<GenericData.Record, Tbl>() {
           @Override
           public void onRemoval(RemovalNotification<GenericData.Record, Tbl> note) {
             emitter.emit(Pair.of(note.getKey(), Pair.of(aggIdx, note.getValue().getValue())));
           }
        }).build(new CacheLoader<GenericData.Record, Tbl>() {
           @Override
           public Tbl load(GenericData.Record record) throws Exception {
             Tbl tbl = config.createTbl();
             tbl.initialize(provider);
             return tbl;
           }
        });
  }

  public void update(GenericData.Record key, Obs obs) {
    cache.asMap().get(key).add(obs);
  }

  public void flush() {
    cache.cleanUp();
  }
}
