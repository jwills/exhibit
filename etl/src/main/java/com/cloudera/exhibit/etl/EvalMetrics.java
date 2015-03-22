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
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.sql.SQLCalculator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.types.avro.AvroType;

import java.util.List;

public class EvalMetrics {

  private List<MetricConfig> metrics;

  public EvalMetrics(List<MetricConfig> metrics) {
    this.metrics = metrics;
  }

  public PCollection<GenericData.Record> apply(PCollection<GenericRecord> input) {
    Schema inputSchema = ((AvroType) input.getPType()).getSchema();
    ExhibitDescriptor descriptor = AvroExhibit.createDescriptor(inputSchema);
    for (MetricConfig mc : metrics) {
    }
    return null;
  }

  static class EvalFn extends MapFn<GenericRecord, GenericData.Record> {

    private final List<MetricConfig> metrics;
    private final String schemaJson;
    private transient Schema schema;

    public EvalFn(List metrics, Schema schema) {
      this.metrics = metrics;
      this.schemaJson = schema.toString();
    }

    @Override
    public void initialize() {
      schema = (new Schema.Parser()).parse(schemaJson);
    }

    @Override
    public GenericData.Record map(GenericRecord genericRecord) {
      GenericData.Record res = new GenericData.Record(schema);
      Exhibit exhibit = AvroExhibit.create(genericRecord);
      // Do metrics computations
      // convert Obs chain back into GenericRecord
      // emit.

      return res;
    }
  }
}
