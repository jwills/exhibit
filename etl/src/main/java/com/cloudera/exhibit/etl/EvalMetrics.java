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

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

import java.util.List;

public class EvalMetrics {

  private List<MetricConfig> metrics;

  public EvalMetrics(List<MetricConfig> metrics) {
    this.metrics = metrics;
  }

  public PCollection<GenericData.Record> apply(PCollection<GenericRecord> input) {
    Schema inputSchema = ((AvroType) input.getPType()).getSchema();
    ExhibitDescriptor descriptor = AvroExhibit.createDescriptor(inputSchema);
    List<Schema.Field> fields = Lists.newArrayList();
    // Copy over input attributes
    for (ObsDescriptor.Field of : descriptor.attributes()) {
      fields.add(new Schema.Field(of.name, AvroExhibit.getSchema(of.type), "", null));
    }
    // Metric column definitions
    for (MetricConfig mc : metrics) {
      Calculator oc = mc.getCalculator();
      ObsDescriptor od = oc.initialize(descriptor);
      for (ObsDescriptor.Field of : od) {
        fields.add(new Schema.Field(of.name, AvroExhibit.getSchema(of.type), "", null));
      }
    }
    //TODO: better naming here
    Schema outputSchema = Schema.createRecord("name", "", "namespace", false);
    outputSchema.setFields(fields);
    PType<GenericData.Record> outType = Avros.generics(outputSchema);
    return input.parallelDo(new EvalFn(metrics, inputSchema, outputSchema), outType);
  }

  static class EvalFn extends MapFn<GenericRecord, GenericData.Record> {

    private final List<MetricConfig> metrics;
    private final String inputSchemaJson;
    private final String outputSchemaJson;
    private transient Schema outputSchema;
    private transient List<Calculator> calculators;

    public EvalFn(List<MetricConfig> metrics, Schema inputSchema, Schema outputSchema) {
      this.metrics = metrics;
      this.inputSchemaJson = inputSchema.toString();
      this.outputSchemaJson = outputSchema.toString();
    }

    @Override
    public void initialize() {
      Schema.Parser parser = new Schema.Parser();
      Schema inputSchema = parser.parse(inputSchemaJson);
      ExhibitDescriptor descriptor = AvroExhibit.createDescriptor(inputSchema);
      this.outputSchema = parser.parse(outputSchemaJson);
      this.calculators = Lists.newArrayList();
      for (MetricConfig mc : metrics) {
        Calculator oc = mc.getCalculator();
        oc.initialize(descriptor);
        calculators.add(oc);
      }
    }

    @Override
    public GenericData.Record map(GenericRecord genericRecord) {
      GenericData.Record res = new GenericData.Record(outputSchema);
      Exhibit exhibit = AvroExhibit.create(genericRecord);
      for (ObsDescriptor.Field f : exhibit.descriptor().attributes()) {
        res.put(f.name, exhibit.attributes().get(f.name));
      }
      for (Calculator oc : calculators) {
        Obs obs = Iterables.getOnlyElement(oc.apply(exhibit));
        for (int i = 0; i < obs.descriptor().size(); i++) {
          res.put(obs.descriptor().get(i).name, obs.get(i));
        }
      }
      return res;
    }

    @Override
    public void cleanup(Emitter<GenericData.Record> emitter) {
      for (Calculator oc : calculators) {
        oc.cleanup();
      }
      calculators.clear();
    }
  }
}
