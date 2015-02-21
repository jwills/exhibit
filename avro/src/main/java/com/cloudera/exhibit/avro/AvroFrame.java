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

public class AvroFrame implements Frame {

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
