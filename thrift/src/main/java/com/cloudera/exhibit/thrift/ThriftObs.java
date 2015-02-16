package com.cloudera.exhibit.thrift;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Obs;
import org.apache.thrift.TBase;

public class ThriftObs extends Obs {

  private final ThriftObsDescriptor descriptor;
  private final TBase tBase;

  public ThriftObs(ThriftObsDescriptor descriptor, TBase tBase) {
    this.descriptor = descriptor;
    this.tBase = tBase;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public Object get(int index) {
    return descriptor.getFieldValue(index, tBase);
  }
}
