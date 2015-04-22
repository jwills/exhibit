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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;
import java.util.List;

public class AvroFrame extends Frame {

  private final ObsDescriptor descriptor;
  private final List<Obs> records;

  public AvroFrame(ObsDescriptor descriptor) {
    this(descriptor, ImmutableList.<GenericRecord>of());
  }

  public AvroFrame(List<? extends GenericRecord> records) {
    this(new AvroObsDescriptor(records.get(0).getSchema()), records);
  }

  public AvroFrame(final ObsDescriptor descriptor, List<? extends GenericRecord> records) {
    this.descriptor = descriptor;
    this.records = ImmutableList.copyOf(Lists.transform(records, new Function<GenericRecord, Obs>() {
      @Override
      public Obs apply(GenericRecord genericRecord) {
        return new AvroObs(descriptor, genericRecord);
      }
    }));
  }

  private static SimpleObs createObs(ObsDescriptor desc, GenericRecord rec) {
    AvroObs aobs = new AvroObs(desc, rec);
    List<Object> values = Lists.newArrayListWithExpectedSize(desc.size());
    for (int i = 0; i < desc.size(); i++) {
      values.add(aobs.get(i));
    }
    return new SimpleObs(desc, values);
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    return records.size();
  }

  @Override
  public Obs get(int index) {
    return records.get(index);
  }

  @Override
  public Iterator<Obs> iterator() {
    return records.iterator();
  }
}
