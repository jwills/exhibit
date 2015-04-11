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

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class OutputGen {

  private final OutputConfig config;
  private final Schema keySchema;
  private final List<Schema> valueSchemas;

  public OutputGen(OutputConfig config, ExhibitDescriptor descriptor) {
    this.config = config;
    this.keySchema = keySchema(descriptor);
    this.valueSchemas = valueSchemas(descriptor);
  }

  public Schema getKeySchema() {
    return keySchema;
  }

  public List<Schema> getValueSchemas() {
    return valueSchemas;
  }

  private Schema keySchema(ExhibitDescriptor descriptor) {
    List<Schema.Field> keyFields = Lists.newArrayList();
    ObsDescriptor od = descriptor.attributes();
    for (String attr : config.attrs) {
      ObsDescriptor.Field f = od.get(od.indexOf(attr));
      keyFields.add(AvroExhibit.getSchemaField(f));
    }

    // Define the key fields for the schema
    List<Schema> frameKeySchemas = Lists.newArrayList();
    for (int i = 0; i < config.keys.size(); i++) {
      frameKeySchemas.add(null);
    }
    for (AggConfig ac : config.aggregates) {
      ObsDescriptor fd = ac.getFrameDescriptor(descriptor);
      for (int i = 0; i < ac.keys.size(); i++) {
        ObsDescriptor.Field f = fd.get(fd.indexOf(ac.keys.get(i)));
        Schema s = AvroExhibit.getSchema(f.type);
        if (frameKeySchemas.get(i) == null) {
          frameKeySchemas.set(i, s);
        } else if (!frameKeySchemas.get(i).equals(s)) {
          throw new IllegalStateException("Mismatched key schemas in AggConfig: find way to auto-resolve");
        }
      }
    }
    for (int i = 0; i < config.keys.size(); i++) {
      keyFields.add(new Schema.Field(config.keys.get(i), frameKeySchemas.get(i), "", null));
    }
    return Schema.createRecord(keyFields);
  }

  private List<Schema> valueSchemas(ExhibitDescriptor descriptor) {
    List<Schema> schemas = Lists.newArrayList();
    for (AggConfig ac : config.aggregates) {
      ObsDescriptor fd = ac.getFrameDescriptor(descriptor);
      List<Schema.Field> fields = Lists.newArrayList();
      for (Map.Entry<String, String> e : ac.values.entrySet()) {
        ObsDescriptor.Field f = fd.get(fd.indexOf(e.getKey()));
        fields.add(new Schema.Field(e.getValue(), AvroExhibit.getSchema(f.type), "", null));
      }
      schemas.add(Schema.createRecord(fields));
    }
    return schemas;
  }

  public PTable<GenericData.Record, Pair<Integer, GenericData.Record>> apply(
      PCollection<Exhibit> exhibits) {
    AvroType<GenericData.Record> kt = Avros.generics(keySchema);
    AvroType<GenericData.Record> vt = Avros.generics(Schema.createUnion(valueSchemas));
    return exhibits.parallelDo(new MapOutFn(config, keySchema, valueSchemas),
        Avros.tableOf(kt, Avros.pairs(Avros.ints(), vt)));
  }

  static class MapOutFn extends DoFn<Exhibit, Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> {

    private final OutputConfig config;
    private final String keyJson;
    private final List<String> valueJson;
    private transient Schema key;
    private transient List<Schema> valueSchemas;
    private transient List<Calculator> calcs;
    private boolean initialized = false;

    public MapOutFn(OutputConfig config, Schema keySchema, List<Schema> valueSchemas) {
      this.config = config;
      this.keyJson = keySchema.toString();
      this.valueJson = Lists.newArrayList(Lists.transform(valueSchemas, new Function<Schema, String>() {
        @Override
        public String apply(Schema schema) {
          return schema.toString();
        }
      }));
    }

    @Override
    public void initialize() {
      this.key = SchemaUtil.getOrParse(key, keyJson);
      this.valueSchemas = Lists.newArrayList(Lists.transform(valueJson, new Function<String, Schema>() {
        @Override
        public Schema apply(@Nullable String s) {
          return SchemaUtil.getOrParse(null, s);
        }
      }));
      this.calcs = Lists.newArrayList();
      for (AggConfig ac : config.aggregates) {
        calcs.add(ac.getCalculator());
      }
      this.initialized = false;
    }

    @Override
    public void process(Exhibit exhibit, Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter) {
      //TODO: compose more complex keys
      GenericData.Record keyRec = new GenericData.Record(key);
      // Copy attributes
      for (String attr : config.attrs) {
        keyRec.put(attr, exhibit.attributes().get(attr));
      }

      if (!initialized) {
        for (Calculator c : calcs) {
          c.initialize(exhibit.descriptor());
        }
        initialized = true;
      }

      for (int i = 0; i < calcs.size(); i++) {
        AggConfig ac = config.aggregates.get(i);
        GenericData.Record valRec = new GenericData.Record(valueSchemas.get(i));
        for (Obs obs : calcs.get(i).apply(exhibit)) {
          for (int j = 0; j < ac.keys.size(); j++) {
            keyRec.put(config.keys.get(j), obs.get(ac.keys.get(j)));
          }

          //TODO: more complex, based on the type of the AggConfig
          for (Map.Entry<String, String> e : ac.values.entrySet()) {
            valRec.put(e.getValue(), obs.get(e.getKey()));
          }
        }
        emitter.emit(Pair.of(keyRec, Pair.of(i, valRec)));
      }
    }
  }
}
