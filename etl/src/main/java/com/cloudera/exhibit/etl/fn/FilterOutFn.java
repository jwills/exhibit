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

import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public class FilterOutFn extends DoFn<Pair<Integer, GenericData.Record>, GenericData.Record> {

  private final int outputIndex;

  public FilterOutFn(int outputIndex) {
    this.outputIndex = outputIndex;
  }

  @Override
  public void process(Pair<Integer, GenericData.Record> input, Emitter<GenericData.Record> emitter) {
    if (outputIndex == input.first()) {
      emitter.emit((GenericData.Record) input.second().get("value"));
    }
  }
}
