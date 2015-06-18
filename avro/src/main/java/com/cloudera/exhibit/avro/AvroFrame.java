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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class AvroFrame extends Frame {

  private AvroObsDescriptor descriptor;
  private List<AvroObs> records;

  public AvroFrame(AvroObsDescriptor descriptor) {
    this(descriptor, ImmutableList.<GenericRecord>of());
  }

  public AvroFrame(List<? extends GenericRecord> records) {
    this(new AvroObsDescriptor(records.get(0).getSchema()), records);
  }

  public AvroFrame(final AvroObsDescriptor descriptor, List<? extends GenericRecord> records) {
    this.descriptor = descriptor;
    this.records = ImmutableList.copyOf(Lists.transform(records, new Function<GenericRecord, AvroObs>() {
      @Override
      public AvroObs apply(GenericRecord genericRecord) {
        return new AvroObs(descriptor, genericRecord);
      }
    }));
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
    return Iterators.transform(records.iterator(), new Function<AvroObs, Obs>() {
      @Override
      public Obs apply(AvroObs obs) {
        return obs;
      }
    });
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.writeObject(descriptor);
    out.writeInt(records.size());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter writer = new GenericDatumWriter(descriptor.schema());
    for (int i = 0; i < records.size(); i++) {
      writer.write(records.get(i).record(), encoder);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    descriptor = (AvroObsDescriptor) in.readObject();
    int sz = in.readInt();
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
    GenericDatumReader reader = new GenericDatumReader(descriptor.schema());
    this.records = Lists.newArrayListWithExpectedSize(sz);
    for (int i = 0; i < sz; i++) {
      records.add(new AvroObs(descriptor, (GenericRecord) reader.read(null, decoder)));
    }
  }
}
