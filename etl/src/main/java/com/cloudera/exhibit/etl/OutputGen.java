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

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.config.AggConfig;
import com.cloudera.exhibit.etl.config.OutputConfig;
import com.cloudera.exhibit.etl.tbl.Tbl;
import com.cloudera.exhibit.etl.tbl.TblCache;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.Set;

import static com.cloudera.exhibit.etl.SchemaUtil.unionValueSchema;

public class OutputGen {

  private final int id;
  private final OutputConfig config;
  private final Schema keySchema;
  private List<SchemaProvider> schemaProviders;

  public OutputGen(int id, OutputConfig config, ExhibitDescriptor descriptor) {
    this.id = id;
    this.config = config;
    this.keySchema = keySchema(descriptor);
    this.schemaProviders = Lists.newArrayList();
    buildSchemas(descriptor);
  }

  public Schema getKeySchema() {
    return keySchema;
  }

  public List<SchemaProvider> getSchemaProviders() {
    return schemaProviders;
  }

  private static List<String> getKeys(AggConfig ac, OutputConfig config) {
    if (ac.keys == null || ac.keys.isEmpty()) {
      return config.keys;
    } else if (config.keys.size() != ac.keys.size()) {
      throw new IllegalArgumentException("Mismatch in aggregate and frame level key settings for frame: " + ac.frame);
    }
    return ac.keys;
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
      List<String> keys = getKeys(ac, config);
      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i).toLowerCase();
        int index = fd.indexOf(key);
        if (index < 0) {
          throw new IllegalArgumentException("Could not find key: " + key + " in obs: " + fd);
        }
        ObsDescriptor.Field f = fd.get(index);
        Schema s = AvroExhibit.getSchema(f.type);
        if (frameKeySchemas.get(i) == null) {
          frameKeySchemas.set(i, s);
        } else if (!frameKeySchemas.get(i).equals(s)) {
          String msg = "Mismatched key schemas in AggConfig:\n";
          msg += frameKeySchemas.get(i).toString() + "\nvs.\n" + s.toString();
          throw new IllegalStateException(msg);
        }
      }
    }
    for (int i = 0; i < config.keys.size(); i++) {
      keyFields.add(new Schema.Field(config.keys.get(i), frameKeySchemas.get(i), "", null));
    }
    Schema wrapper = Schema.createRecord("ExK" + id, "", "exhibit", false);
    wrapper.setFields(keyFields);
    return wrapper;
  }

  private void buildSchemas(ExhibitDescriptor descriptor) {
    for (int i = 0; i < config.aggregates.size(); i++) {
      AggConfig ac = config.aggregates.get(i);
      ObsDescriptor fd = ac.getFrameDescriptor(descriptor);
      List<Schema.Field> fields = Lists.newArrayList();

      if (ac.values == null || ac.values.isEmpty()) {
        // All of the non-key fields are treated as values
        ac.values = Maps.newHashMap();
        Set<String> keys = Sets.newHashSet(getKeys(ac, config));
        for (ObsDescriptor.Field f : fd) {
          if (!keys.contains(f.name)) {
            ac.values.put(f.name, f.name);
          }
        }
      }

      Tbl tbl = ac.createTbl();
      this.schemaProviders.add(tbl.getSchemas(fd, id, i));
    }
  }

  public PTable<GenericData.Record, Pair<Integer, GenericData.Record>> apply(
      PCollection<Exhibit> exhibits) {
    List<Schema> interSchemas = Lists.newArrayList();
    for (SchemaProvider sp : schemaProviders) {
      interSchemas.add(sp.get(0));
    }
    AvroType<GenericData.Record> kt = Avros.generics(keySchema);
    AvroType<GenericData.Record> vt = Avros.generics(unionValueSchema("OutGen" + id, interSchemas));
    return exhibits.parallelDo(new MapOutFn(id, config, keySchema, schemaProviders),
        Avros.tableOf(kt, Avros.pairs(Avros.ints(), vt)));
  }

  static class MapOutFn extends DoFn<Exhibit, Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> {

    private final int outputId;
    private final OutputConfig config;
    private final String keyJson;
    private final List<SchemaProvider> providers;
    private transient Schema key;
    private transient List<TblCache> tblCaches;
    private transient List<Calculator> calcs;
    private boolean initialized = false;

    public MapOutFn(int outputId, OutputConfig config, Schema keySchema, List<SchemaProvider> providers) {
      this.outputId = outputId;
      this.config = config;
      this.keyJson = keySchema.toString();
      this.providers = providers;
    }

    @Override
    public void initialize() {
      this.key = SchemaUtil.getOrParse(key, keyJson);
      this.calcs = Lists.newArrayList();
      this.tblCaches = Lists.newArrayList();
      this.initialized = false;
    }

    @Override
    public void process(Exhibit exhibit, Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter) {
      if (!initialized) {
        for (int i = 0; i < config.aggregates.size(); i++) {
          AggConfig ac = config.aggregates.get(i);
          Calculator c = ac.getCalculator();
          c.initialize(exhibit.descriptor());
          calcs.add(c);
          TblCache tc = new TblCache(ac, i, emitter, providers.get(i));
          tblCaches.add(tc);
        }
        initialized = true;
      }

      for (int i = 0; i < calcs.size(); i++) {
        AggConfig ac = config.aggregates.get(i);
        for (Obs obs : calcs.get(i).apply(exhibit)) {
          List<String> keys = getKeys(ac, config);
          GenericData.Record keyRec = new GenericData.Record(key);
          // Copy attributes
          for (String attr : config.attrs) {
            keyRec.put(attr, exhibit.attributes().get(attr));
          }
          for (int j = 0; j < keys.size(); j++) {
            keyRec.put(config.keys.get(j), obs.get(keys.get(j)));
          }
          tblCaches.get(i).update(keyRec, obs);
          increment("Exhibit", "Calc" + outputId + "_" + i);
        }
      }
    }

    @Override
    public void cleanup(Emitter<Pair<GenericData.Record, Pair<Integer, GenericData.Record>>> emitter) {
      for (TblCache tc : tblCaches) {
        tc.flush();
      }
      tblCaches.clear();
    }
  }
}
