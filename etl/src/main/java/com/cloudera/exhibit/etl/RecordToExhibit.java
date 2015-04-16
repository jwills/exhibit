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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.composite.UpdatableExhibit;
import com.cloudera.exhibit.core.composite.UpdatableExhibitDescriptor;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.etl.config.FrameConfig;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

import java.util.List;

public class RecordToExhibit  {

  private List<FrameConfig> metrics;

  public RecordToExhibit(List<FrameConfig> metrics) {
    this.metrics = metrics;
  }

  public ExhibitDescriptor getDescriptor(PType<GenericRecord> ptype) {
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

  public PCollection<Exhibit> apply(PCollection<GenericRecord> records) {
    Schema s = ((AvroType) records.getPType()).getSchema();
    return records.parallelDo("recordToExhibit", new RecordToExhibitFn(s, metrics),
            Avros.derivedImmutable(Exhibit.class,
                    new NoOpMapFn<GenericData.Record, Exhibit>(),
                    new NoOpMapFn<Exhibit, GenericData.Record>(),
                    Avros.generics(s)));
  }

  static class RecordToExhibitFn extends MapFn<GenericRecord, Exhibit> {

    private final String schemaJson;
    private final List<FrameConfig> metrics;
    private transient Schema schema;
    private transient List<Calculator> calcs;
    private transient UpdatableExhibitDescriptor descriptor;

    public RecordToExhibitFn(Schema schema, List<FrameConfig> metrics) {
      this.schemaJson = schema.toString();
      this.metrics = metrics;
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
    }

    @Override
    public Exhibit map(GenericRecord genericRecord) {
      UpdatableExhibit ue = new UpdatableExhibit(AvroExhibit.create(genericRecord));
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
