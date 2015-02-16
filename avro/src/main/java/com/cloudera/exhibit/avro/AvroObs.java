package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Obs;
import org.apache.avro.generic.IndexedRecord;

public class AvroObs extends Obs {

  private IndexedRecord record;
  private AvroObsDescriptor descriptor;

  public AvroObs(IndexedRecord record) {
    this.record = record;
    this.descriptor = new AvroObsDescriptor(record.getSchema());
  }

  @Override
  public ObsDescriptor descriptor() {
    return new AvroObsDescriptor(record.getSchema());
  }

  @Override
  public Object get(int index) {
    if (descriptor.get(index).type == ObsDescriptor.FieldType.STRING) {
      Object r = record.get(index);
      return r == null ? null : r.toString();
    } else {
      return record.get(index);
    }
  }
}
