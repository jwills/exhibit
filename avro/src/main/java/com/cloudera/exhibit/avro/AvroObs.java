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
package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Obs;
import org.apache.avro.generic.GenericRecord;

public class AvroObs extends Obs {

  private GenericRecord record;
  private ObsDescriptor descriptor;

  public AvroObs(GenericRecord record) {
    this(new AvroObsDescriptor(record.getSchema()), record);
  }

  public AvroObs(ObsDescriptor descriptor, GenericRecord record) {
    this.descriptor = descriptor;
    this.record = record;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public Object get(int index) {
    ObsDescriptor.Field f = descriptor.get(index);
    Object r = record.get(f.name);
    if (f.type == ObsDescriptor.FieldType.STRING) {
      return r == null ? null : r.toString();
    } else {
      return r;
    }
  }

}
