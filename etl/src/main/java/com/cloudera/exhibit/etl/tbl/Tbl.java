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
package com.cloudera.exhibit.etl.tbl;

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import org.apache.avro.generic.GenericData;

import java.util.List;

public interface Tbl {
  // How many rows of output will the finalize method generate?
  int arity();

  // Generate the schemas (intermediate and output) for a given set of inputs.
  // Do this first on the client-side
  SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx);

  // Inside of a map/reduce phase: initialize a Tbl using the SchemaProvider
  // we generated on the client
  void initialize(SchemaProvider provider);

  // Add a single additional observation
  void add(Obs obs);

  // Get the current (intermediate) state of this table
  GenericData.Record getValue();

  // Merge two intermediate states of this table type together
  GenericData.Record  merge(GenericData.Record current, GenericData.Record next);

  // In the reduce phase, "finalize" the output-- transform it from its
  // intermediate form to its final output form.
  List<GenericData.Record> finalize(GenericData.Record value);
}
