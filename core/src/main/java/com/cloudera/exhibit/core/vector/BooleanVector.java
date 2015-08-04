package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.google.common.primitives.Booleans;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BooleanVector extends Vector {

  private boolean [] values;
  private int size;

  protected BooleanVector(){
    this(Collections.emptyList());
  }

  // Construct which avoids the copy of data
  public BooleanVector(final boolean[] arr) {
    super(FieldType.BOOLEAN);
    size = arr.length;
    values = arr;
  }

  protected BooleanVector(List<Object> values) {
    super(FieldType.BOOLEAN);
    this.size = values.size();
    this.values = new boolean[this.size];
    int idx = 0;
    for(Object o: values) {
      if(!(o instanceof Boolean)){
        throw new IllegalArgumentException("Received non-boolean value" + o.toString() );
      }
      this.values[idx] = (Boolean)o;
      idx++;
    }
  }

  @Override
  public Boolean get(int index) {
    return values[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<Object> iterator() {
    return ((List)Booleans.asList(values)).iterator();
  }
}
