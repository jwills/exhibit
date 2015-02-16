package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.avro.generic.IndexedRecord;

import java.util.Iterator;
import java.util.List;

public class AvroFrame implements Frame {

  private final AvroObsDescriptor descriptor;
  private final List<? extends IndexedRecord> records;

  public AvroFrame(AvroObsDescriptor descriptor) {
    this.descriptor = descriptor;
    this.records = ImmutableList.of();
  }

  public AvroFrame(List<? extends IndexedRecord> records) {
    this.descriptor = new AvroObsDescriptor(records.get(0).getSchema());
    this.records = records;
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
    return new AvroObs(records.get(index));
  }

  @Override
  public Iterator<Obs> iterator() {
    return Iterators.transform(records.iterator(), new Function<IndexedRecord, Obs>() {
      @Override
      public Obs apply(IndexedRecord indexedRecord) {
        return new AvroObs(indexedRecord);
      }
    });
  }
}
