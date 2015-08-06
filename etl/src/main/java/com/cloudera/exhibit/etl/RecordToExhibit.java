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
import com.cloudera.exhibit.avro.AvroFrame;
import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.composite.UpdatableExhibit;
import com.cloudera.exhibit.core.composite.UpdatableExhibitDescriptor;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.etl.config.FrameConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.ReadableData;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Code for converting a {@link PCollection} of {@link GenericData.Record} instances into
 * a {@code PCollection} of {@link Exhibit} instances that include both a) any in-memory
 * frames that should be used for every {@code Exhibit} record and any temporary frames
 * that need to be computed prior to generating the output frames.
 */
public class RecordToExhibit  {

  private Map<String, ReadableData<GenericData.Record>> readables;
  private List<FrameConfig> metrics;

  public RecordToExhibit(Map<String, ReadableData<GenericData.Record>> readables, List<FrameConfig> metrics) {
    this.readables = readables;
    this.metrics = metrics;
  }

  public ExhibitDescriptor getDescriptor(PType<GenericData.Record> ptype) {
    Schema schema = ((AvroType) ptype).getSchema();
    return getDescriptor(schema, metrics);
  }

  private static UpdatableExhibitDescriptor getDescriptor(Schema schema, List<FrameConfig> metrics) {
    UpdatableExhibitDescriptor descriptor = new UpdatableExhibitDescriptor(
            AvroExhibit.createDescriptor(schema));
    for (int i = 0; i < metrics.size(); i++) {
      Calculator c = metrics.get(i).getCalculator();
      ObsDescriptor od = c.initialize(descriptor);
      descriptor.add(metrics.get(i).name, od);
    }
    return descriptor;
  }

  private static class NoOpMapFn<S, T> extends MapFn<S, T> {
    @Override
    public T map(S s) {
      return (T) s;
    }
  }

  public PCollection<Exhibit> apply(PCollection<GenericData.Record> records) {
    Schema s = ((AvroType) records.getPType()).getSchema();
    //TODO: real serialization scheme for Exhibits
    return records.parallelDo("recordToExhibit", new RecordToExhibitFn(s, readables, metrics),
            Avros.derivedImmutable(Exhibit.class,
                    new NoOpMapFn<GenericData.Record, Exhibit>(),
                    new NoOpMapFn<Exhibit, GenericData.Record>(),
                    Avros.generics(s)));
  }

  static class RecordToExhibitFn extends MapFn<GenericData.Record, Exhibit> {

    private final String schemaJson;
    private final Map<String, ReadableData<GenericData.Record>> readables;
    private final List<FrameConfig> metrics;
    private transient Schema schema;
    private transient List<Calculator> calcs;
    private transient UpdatableExhibitDescriptor descriptor;
    private transient Map<String, Frame> readFrames;

    public RecordToExhibitFn(Schema schema, Map<String, ReadableData<GenericData.Record>> readables, List<FrameConfig> metrics) {
      this.schemaJson = schema.toString();
      this.readables = readables;
      this.metrics = metrics;
    }

    @Override
    public void configure(Configuration conf) {
      for (ReadableData<GenericData.Record> rd : readables.values()) {
        rd.configure(conf);
      }
    }

    @Override
    public void initialize() {
      this.schema = SchemaUtil.getOrParse(this.schema, schemaJson);
      this.descriptor = getDescriptor(schema, metrics);
      this.calcs = Lists.newArrayList();
      for (FrameConfig mc : metrics) {
        Calculator c = mc.getCalculator();
        c.initialize(descriptor);
        calcs.add(c);
      }
      if (readFrames == null) {
        readFrames = Maps.newHashMap();
        for (Map.Entry<String, ReadableData<GenericData.Record>> e : readables.entrySet()) {
          ReadableData<GenericData.Record> rd = e.getValue();
          try {
            List<GenericData.Record> records = Lists.newArrayList(rd.read(getContext()));
            //TODO: allow empty frames
            readFrames.put(e.getKey(), new AvroFrame(records));
          } catch (IOException e1) {
            throw new CrunchRuntimeException(e1);
          }
        }
      }
    }

    @Override
    public Exhibit map(GenericData.Record genericRecord) {
      UpdatableExhibit ue = new UpdatableExhibit(AvroExhibit.create(genericRecord));
      ue.addAllFrames(readFrames);
      for (int i = 0; i < calcs.size(); i++) {
        String name = metrics.get(i).name;
        Iterable<Obs> res = calcs.get(i).apply(ue);
        if (res instanceof Frame) {
          ue.add(name, (Frame) res);
        } else {
          Frame f = new SimpleFrame(descriptor.frames().get(name), Lists.newArrayList(res));
          ue.add(name, f);
        }
      }
      return ue;
    }
  }
}
