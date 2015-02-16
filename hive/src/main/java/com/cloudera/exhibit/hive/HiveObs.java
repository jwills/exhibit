package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Obs;

class HiveObs extends Obs {

  private final ObsDescriptor descriptor;
  private final Object[] values;

  public HiveObs(HiveObsDescriptor descriptor, Object listElement) {
    this.descriptor = descriptor;
    this.values = descriptor.convert(listElement);
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public Object get(int index) {
    return values[index];
  }
}
