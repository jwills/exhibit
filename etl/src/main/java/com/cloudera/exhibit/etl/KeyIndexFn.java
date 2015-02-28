/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.exhibit.etl;

import com.google.common.collect.Sets;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.Union;

import java.util.Set;

public class KeyIndexFn<R extends GenericRecord> extends DoFn<R, Pair<Object, Pair<Integer, Union>>> {

  private final Set<String> keyFields;
  private final Set<String> filteredKeys;
  private final int index;

  public KeyIndexFn(Set<String> keyFields, Set<String> filteredKeys, int index) {
    this.keyFields = Sets.newHashSet(keyFields);
    this.filteredKeys = Sets.newHashSet(filteredKeys);
    this.index = index;
  }

  @Override
  public void process(R r, Emitter<Pair<Object, Pair<Integer, Union>>> emitter) {
    Pair<Integer, R> result = Pair.of(index, r);
    if (r != null) {
      for (String field : keyFields) {
        Object key = r.get(field);
        if (key != null) {
          if (!filteredKeys.contains(key.toString())) {
            emitter.emit(Pair.of(key, Pair.of(index, new Union(index, result))));
          }
        }
      }
    }
  }
}
