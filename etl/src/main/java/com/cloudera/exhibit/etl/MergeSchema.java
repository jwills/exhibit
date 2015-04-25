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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.etl.config.SourceConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.cloudera.exhibit.etl.SchemaUtil.unwrapNull;

public class MergeSchema implements Serializable {

  private final String name;
  private final String keyField;
  private final String keySchemaJson;
  private final List<SourceConfig> sources;
  private final int parallelism;

  MergeSchema(String name, String keyField, Schema keySchema, List<SourceConfig> sources, int parallelism) {
    this.name = name;
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
      fieldNames.put(keyField.toLowerCase(), sf);
    }
    for (SourceConfig sc : sources) {
      Schema fs = sc.getSchema();
      if (sc.embedded) {
        for (Schema.Field sf : fs.getFields()) {
          String lookup = sf.name().toLowerCase();
          Schema.Field nsf = new Schema.Field(sf.name(), sf.schema(), null, null);
          if (!fieldNames.containsKey(lookup)) {
            ret.add(nsf);
            fieldNames.put(lookup, nsf);
          } else {
            if (!unwrapNull(nsf.schema()).equals(unwrapNull(fieldNames.get(lookup).schema()))) {
              throw new IllegalStateException("Mismatched schemas for field " + lookup + ": " +
                      nsf.schema() + " vs. " + fieldNames.get(lookup).schema());
            }
          }
        }
      } else {
        if (sc.repeated) {
          fs = Schema.createArray(fs);
        }
        if (sc.nullable) {
          fs = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), fs));
        }
        String lookup = sc.name.toLowerCase();
        if (!fieldNames.containsKey(lookup)) {
          Schema.Field sf = new Schema.Field(sc.name, fs, null, null);
          ret.add(sf);
          fieldNames.put(lookup, sf);
        } else {
          if (!fs.equals(fieldNames.get(lookup).schema())) {
            throw new IllegalStateException("Mismatched schemas for field " + lookup + " : " +
                    fs + " vs. " + fieldNames.get(lookup).schema());
          }
        }
      }
    }
    Schema rec = Schema.createRecord(name, "", "", false);
    rec.setFields(ret);
    return rec;
  }

  public PCollection<GenericData.Record> apply(PTable<Object, Pair<Integer, GenericData.Record>> input) {
    Schema out = createOutputSchema();
    return SecondarySort.sortAndApply(input, new SSFn(out), Avros.generics(out), parallelism);
  }

  private class SSFn extends MapFn<Pair<Object, Iterable<Pair<Integer, GenericData.Record>>>, GenericData.Record> {

    private String schemaJson;
    private transient Schema schema;
    private List<PType> ptypes;
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
    public GenericData.Record map(Pair<Object, Iterable<Pair<Integer, GenericData.Record>>> input) {
      long start = System.currentTimeMillis();
      GenericData.Record ret = new GenericData.Record(schema);
      if (keyField != null) {
        ret.put(keyField, input.first());
      }
      int records = 0;
      for (Pair<Integer, GenericData.Record> p : input.second()) {
        int index = p.first();
        GenericData.Record value = (GenericData.Record) p.second().get(0);
        SourceConfig sc = sources.get(index);
        if (sc.embedded) {
          copy(value, ret);
        } else {
          GenericData.Record copy = new GenericData.Record(element(schema.getField(sc.name).schema()));
          copy(value, copy);
          if (sc.repeated) {
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
        records++;
      }
      increment("ExhibitRuntime", "MergeSchemaMsec", System.currentTimeMillis() - start);
      increment("ExhibitRuntime", "MergeSchemaRecords", records);
      return ret;
    }
  }

  private Schema element(Schema base) {
    if (base.getType() == Schema.Type.ARRAY) {
      return element(base.getElementType());
    } else if (base.getType() == Schema.Type.UNION) {
      List<Schema> elems = base.getTypes();
      if (elems.get(0).getType() == Schema.Type.NULL) {
        return element(elems.get(1));
      } else if (elems.get(1).getType() == Schema.Type.NULL) {
        return element(elems.get(0));
      }
    }
    return base;
  }

  private static void copy(GenericData.Record orig, GenericData.Record copy) {
    for (Schema.Field sf : orig.getSchema().getFields()) {
      Object val = orig.get(sf.pos());
      if (val instanceof CharSequence) {
        val = val.toString();
      }
      copy.put(sf.name(), val);
    }
  }
}
