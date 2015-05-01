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

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.config.FrameConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class CollectFn extends DoFn<Exhibit, GenericData.Record> {
  private final FrameConfig frame;
  private final String json;

  private transient Calculator calc;
  private transient Schema schema;
  private boolean initialized;

  public CollectFn(FrameConfig frame, Schema mapsideSchema) {
    this.frame = frame;
    this.json = mapsideSchema.toString();
  }

  @Override
  public void initialize() {
    this.calc = frame.getCalculator();
    this.schema = (new Schema.Parser()).parse(json);
    this.initialized = false;
  }

  @Override
  public void process(Exhibit exhibit, Emitter<GenericData.Record> emitter) {
    if (!initialized) {
      calc.initialize(exhibit.descriptor());
      initialized = true;
    }
    for (Obs obs : calc.apply(exhibit)) {
      GenericData.Record out = new GenericData.Record(schema);
      for (ObsDescriptor.Field f : obs.descriptor()) {
        out.put(f.name, obs.get(f.name));
      }
      emitter.emit(out);
    }
  }

  @Override
  public void cleanup(Emitter<GenericData.Record> emitter) {
    calc.cleanup();
  }
}
