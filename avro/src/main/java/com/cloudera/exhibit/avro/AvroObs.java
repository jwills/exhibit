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
