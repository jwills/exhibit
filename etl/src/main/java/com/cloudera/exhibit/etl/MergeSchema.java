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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Union;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class MergeSchema implements Serializable {

  private final String keyField;
  private final String keySchemaJson;
  private final List<SourceConfig> sources;
  private final int parallelism;

  MergeSchema(String keyField, Schema keySchema, List<SourceConfig> sources, int parallelism) {
    this.keyField = keyField;
    this.keySchemaJson = keySchema.toString();
    this.sources = sources;
    this.parallelism = parallelism;
  }

  Schema createOutputSchema() {
    Schema.Parser p = new Schema.Parser();
    List<Schema.Field> ret = Lists.newArrayList();
    Map<String, Schema.Field> fieldNames = Maps.newHashMap();
    if (keyField != null) {
      Schema.Field sf = new Schema.Field(keyField, p.parse(keySchemaJson), null, null);
      ret.add(sf);
      fieldNames.put(keyField, sf);
    }
    for (SourceConfig sc : sources) {
      Schema fs = sc.getSchema();
      if (sc.embedded) {
        for (Schema.Field sf : fs.getFields()) {
          if (!fieldNames.containsKey(sf.name())) {
            ret.add(sf);
            fieldNames.put(sf.name(), sf);
          } else if (!sf.schema().equals(fieldNames.get(sf.name()).schema())) {
              throw new IllegalStateException("Mismatched schemas for field " + sf.name() + ": " +
                  sf.schema() + " vs. " + fieldNames.get(sf.name()).schema());
          }
        }
      } else {
        if (sc.nullable) {
          fs = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), fs));
        }
        if (sc.repeated) {
          fs = Schema.createArray(fs);
        }
        if (!fieldNames.containsKey(sc.name)) {
          Schema.Field sf = new Schema.Field(sc.name, fs, null, null);
          ret.add(sf);
          fieldNames.put(sc.name, sf);
        } else if (!fs.equals(fieldNames.get(sc.name).schema())) {
          throw new IllegalStateException("Mismatched schemas for field " + sc.name + " : " +
              fs + " vs. " + fieldNames.get(sc.name).schema());
        }
      }
    }
    return Schema.createRecord(ret);
  }

  public PCollection<GenericData.Record> apply(PTable<Object, Pair<Integer, Union>> input) {
    Schema out = createOutputSchema();
    AvroType<GenericData.Record> ret = Avros.generics(out);
    return SecondarySort.sortAndApply(input, new SSFn(out), ret, parallelism);
  }

  private class SSFn extends MapFn<Pair<Object, Iterable<Pair<Integer, Union>>>, GenericData.Record> {

    private String schemaJson;
    private transient Schema schema;
    private List<PType<GenericData.Record>> ptypes;
    public SSFn(Schema out) {
      this.schemaJson = out.toString();
    }

    @Override
    public void initialize() {
      this.schema = (new Schema.Parser()).parse(schemaJson);
      this.ptypes = Lists.newArrayList();
      for (SourceConfig sc : sources) {
        ptypes.add(Avros.generics(sc.getSchema()));
      }
    }

    @Override
    public GenericData.Record map(Pair<Object, Iterable<Pair<Integer, Union>>> input) {
      GenericData.Record ret = new GenericData.Record(schema);
      if (keyField != null) {
        ret.put(keyField, input.first());
      }
      for (Pair<Integer, Union> p : input.second()) {
        int index = p.first();
        GenericData.Record copy = ptypes.get(index).getDetachedValue((GenericData.Record) p.second().getValue());
        SourceConfig sc = sources.get(index);
        if (sc.embedded) {
          for (Schema.Field sf : copy.getSchema().getFields()) {
            ret.put(sf.name(), copy.get(sf.pos()));
          }
        } else if (sc.repeated) {
          List list = (List) ret.get(sc.name);
          if (list == null) {
            list = Lists.newArrayList();
            ret.put(sc.name, list);
          }
          list.add(copy);
        } else {
          ret.put(sc.name, copy);
        }
      }
      return ret;
    }
  }
}
