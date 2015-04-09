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

import com.google.common.collect.Maps;
import org.apache.avro.Schema;

import java.util.Map;

public class SumMapTbl extends AbstractTbl {

  protected SumMapTbl(int id, Schema valueSchema) {
    super(id, Schema.createMap(valueSchema), Schema.createMap(valueSchema));
  }

  @Override
  public Object merge(Object current, Object next) {
    Map<String, Object> map = (Map<String, Object>) current;
    if (map == null) {
      map = Maps.newHashMap();
    }
    Map<String, Object> nmap = (Map<String, Object>) next;
    Schema vschema = intermediateSchema().getValueType();
    for (Map.Entry<String, Object> e : nmap.entrySet()) {
      String key = e.getKey();
      map.put(key, SumTbl.add(map.get(key), e.getValue(), vschema));
    }
    return map;
  }

  @Override
  public Object finalize(Object current) {
    // TODO: add top support
    return current;
  }
}
