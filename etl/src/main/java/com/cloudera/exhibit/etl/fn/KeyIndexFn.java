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
package com.cloudera.exhibit.etl.fn;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.avro.AvroType;

import java.util.List;
import java.util.Set;

public class KeyIndexFn<R extends GenericRecord> extends DoFn<R, Pair<Object, Pair<Integer, GenericData.Record>>> {

  private final AvroType<GenericData.Record> valueType;
  private final Set<String> keyFields;
  private final Set<String> filteredKeys;
  private final int index;

  public KeyIndexFn(AvroType<GenericData.Record> valueType, Set<String> keyFields, Set<String> filteredKeys, int index) {
    this.valueType = valueType;
    this.keyFields = Sets.newHashSet(keyFields);
    this.filteredKeys = Sets.newHashSet(filteredKeys);
    this.index = index;
  }

  @Override
  public void initialize() {
    valueType.initialize(getConfiguration());
  }

  @Override
  public void process(R r, Emitter<Pair<Object, Pair<Integer, GenericData.Record>>> emitter) {
    if (r != null) {
      GenericData.Record value = matchSchema(r);
      GenericData.Record out = new GenericData.Record(valueType.getSchema());
      out.put(0, value);
      for (String field : keyFields) {
        Object key = r.get(field);
        if (key != null) {
          if (!filteredKeys.contains(key.toString())) {
            emitter.emit(Pair.of(key, Pair.of(index, out)));
          }
        }
      }
    }
  }

  private GenericData.Record matchSchema(R r) {
    Schema target = r.getSchema();
    List<Schema> schemas = valueType.getSchema().getFields().get(0).schema().getTypes();
    Schema match = null;
    for (Schema s : schemas) {
      if (s.getFields().size() == target.getFields().size()) {
        boolean matched = true;
        for (int i = 0; i < s.getFields().size(); i++) {
          Schema.Field sf = s.getFields().get(i);
          if (!sf.equals(target.getFields().get(i))) {
            matched = false;
            break;
          }
        }
        if (matched) {
          match = duplicate(s);
          break;
        }
      }
    }
    if (match == null) {
      throw new IllegalStateException("Could not find matching schema for: " + target);
    }
    GenericData.Record ret = new GenericData.Record(match);
    for (int i = 0; i < match.getFields().size(); i++) {
      ret.put(i, r.get(i));
    }
    return ret;
  }

  private Schema duplicate(Schema s) {
    Schema s1 = Schema.createRecord(s.getName(), "", "crunch", false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field sf : s.getFields()) {
      fields.add(new Schema.Field(sf.name(), sf.schema(), sf.doc(), sf.defaultValue(), sf.order()));
    }
    s1.setFields(fields);
    return s1;
  }
}
