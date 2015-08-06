package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.google.common.primitives.Ints;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class IntVector extends Vector {

  private int [] values;
  private int size;

  protected IntVector(){
    this(Collections.emptyList());
  }

  // Construct which avoids the copy of data
  public IntVector(final int[] arr) {
    super(FieldType.INTEGER);
    size = arr.length;
    values = arr;
  }

  protected IntVector(List<Object> values) {
    super(FieldType.INTEGER);
    this.size = values.size();
    this.values = new int[this.size];
    int idx = 0;
    for(Object o: values) {
      if(!(o instanceof Integer)){
        throw new IllegalArgumentException("Received non-int value" + o.toString() );
      }
      this.values[idx] = (Integer)o;
      idx++;
    }
  }

  @Override
  public Integer get(int index) {
    return values[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<Object> iterator() {
    return ((List)Ints.asList(values)).iterator();
  }
}
