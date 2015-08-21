package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public abstract class Vector implements Vec {
  private final FieldType type;

  protected Vector(FieldType type){
    this.type = type;
  }

  @Override
  public FieldType getType(){
    return this.type;
  }
}
