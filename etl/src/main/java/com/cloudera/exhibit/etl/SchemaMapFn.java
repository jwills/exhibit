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
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

public class SchemaMapFn extends
    MapFn<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>,
          Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>>> {

  private final int index;
  private final SchemaProvider provider;

  public SchemaMapFn(int index, SchemaProvider provider) {
    this.index = index;
    this.provider = provider;
  }

  @Override
  public Pair<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> map(
      Pair<GenericData.Record, Pair<Integer, GenericData.Record>> input) {
    int aggIdx = input.second().first();
    GenericData.Record outKey = new GenericData.Record(provider.get(0));
    outKey.put("index", index);
    outKey.put("key", input.first());
    GenericData.Record outValue = new GenericData.Record(provider.get(1));
    outValue.put("value", input.second().second());
    increment("Exhibit", "MappedSchema" + index);
    return Pair.of(Pair.of(outKey, aggIdx), Pair.of(aggIdx, outValue));
  }
}
