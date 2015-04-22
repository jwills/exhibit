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

public interface Tbl {
  SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx);

  void initialize(SchemaProvider provider);
  void add(Obs obs);
  GenericData.Record getValue();
  GenericData.Record  merge(GenericData.Record current, GenericData.Record next);
  GenericData.Record finalize(GenericData.Record value);
}
