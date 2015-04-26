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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.avro.AvroType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KeyIndexFn<R extends GenericRecord> extends DoFn<R, Pair<String, Pair<Integer, GenericData.Record>>> {

  private final AvroType<GenericData.Record> outType;
  private final Set<String> keyFields;
  private final Set<String> filteredKeys;
  private final int index;
  private transient Map<Schema, Schema> matches;

  public KeyIndexFn(AvroType<GenericData.Record> outType, Set<String> keyFields, Set<String> filteredKeys, int index) {
    this.outType = outType;
    this.keyFields = Sets.newHashSet(keyFields);
    this.filteredKeys = Sets.newHashSet(filteredKeys);
    this.index = index;
  }

  @Override
  public void initialize() {
    outType.initialize(getConfiguration());
    matches = Maps.newHashMap();
  }

  @Override
  public void process(R r, Emitter<Pair<String, Pair<Integer, GenericData.Record>>> emitter) {
    long start = System.currentTimeMillis();
    int emitted = 0;
    if (r != null) {
      GenericData.Record out = new GenericData.Record(outType.getSchema());
      out.put(0, matchSchema(r));
      for (String field : keyFields) {
        Object key = r.get(field);
        if (key != null) {
          String skey = key.toString();
          if (!filteredKeys.contains(skey)) {
            emitter.emit(Pair.of(skey, Pair.of(index, out)));
            emitted++;
          }
        }
      }
    }
    increment("ExhibitRuntime", "KeyIndexFnMsec" + index, System.currentTimeMillis() - start);
    increment("ExhibitRuntime", "KeyIndexFnEmits" + index, emitted);
  }

  private GenericData.Record matchSchema(R r) {
    Schema target = r.getSchema();
    Schema match = matches.get(target);
    if (match == null) {
      List<Schema> schemas = outType.getSchema().getFields().get(0).schema().getTypes();
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
      matches.put(target, match);
    }
    GenericData.Record ret = new GenericData.Record(match);
    for (Schema.Field sf : match.getFields()) {
      ret.put(sf.pos(), r.get(sf.pos()));
    }
    return ret;
  }

  private Schema duplicate(Schema s) {
    String ns = (s.getNamespace() == null || s.getNamespace().isEmpty()) ? "crunch" : s.getNamespace();
    Schema s1 = Schema.createRecord(s.getName(), "", ns, false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field sf : s.getFields()) {
      fields.add(new Schema.Field(sf.name(), sf.schema(), sf.doc(), sf.defaultValue(), sf.order()));
    }
    s1.setFields(fields);
    return s1;
  }
}
