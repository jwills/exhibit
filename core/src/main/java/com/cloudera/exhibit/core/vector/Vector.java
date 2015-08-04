package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Vec;

public abstract class Vector implements Vec {
  private FieldType type;

  protected Vector(FieldType type){
    this.type = type;
  }

  public FieldType getType(){
    return this.type;
  }
}
