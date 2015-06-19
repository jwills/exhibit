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

import com.cloudera.exhibit.etl.config.SourceConfig;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.avro.AvroType;

import java.util.List;

public class KeyIndexFn<R extends GenericRecord> extends DoFn<R, Pair<String, Pair<Integer, GenericData.Record>>> {

  private final AvroType<GenericData.Record> outType;
  private final SourceConfig src;
  private final int index;
  private transient Schema match;

  public KeyIndexFn(AvroType<GenericData.Record> outType, SourceConfig src, int index) {
    this.outType = outType;
    this.src = src;
    this.index = index;
  }

  @Override
  public void initialize() {
    outType.initialize(getConfiguration());
    match = matchSchema();
  }

  @Override
  public void process(R r, Emitter<Pair<String, Pair<Integer, GenericData.Record>>> emitter) {
    if (r != null) {
      GenericData.Record out = new GenericData.Record(outType.getSchema());
      GenericData.Record ret = new GenericData.Record(match);
      for (Schema.Field sf : match.getFields()) {
        ret.put(sf.pos(), r.get(sf.name()));
      }
      out.put(0, ret);
      for (String field : src.keyFields) {
        Object key = r.get(field);
        if (key != null) {
          String skey = key.toString();
          if (!src.invalidKeys.contains(skey)) {
            emitter.emit(Pair.of(skey, Pair.of(index, out)));
          }
        } else {
          increment("ExhibitRuntime", "NullGroupingKey");
        }
      }
    } else {
      increment("ExhibitRuntime", "NullInput");
    }
  }

  private Schema matchSchema() {
    Schema target = src.getSchema();
    Schema match = null;
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
    return match;
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
