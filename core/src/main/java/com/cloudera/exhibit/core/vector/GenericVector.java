package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GenericVector extends Vector  {
  private List<Object> values;

  public GenericVector(FieldType fieldType){
    this(fieldType, Collections.emptyList());
  }

  public GenericVector(FieldType fieldType, List values){
    super(fieldType);
    this.values = values;
  }

  @Override
  public Object get(int index) {
    return values.get(index);
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public Iterator<Object> iterator() {
    return values.iterator();
  }
}

