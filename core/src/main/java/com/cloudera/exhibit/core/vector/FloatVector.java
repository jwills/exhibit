package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.google.common.primitives.Floats;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class FloatVector extends Vector {

  private float [] values;
  private int size;

  protected FloatVector(){
    this(Collections.emptyList());
  }

  // Construct which avoids the copy of data
  public FloatVector(final float[] arr) {
    super(FieldType.FLOAT);
    size = arr.length;
    values = arr;
  }

  protected FloatVector(List<Object> values) {
    super(FieldType.FLOAT);
    this.size = values.size();
    this.values = new float[this.size];
    int idx = 0;
    for(Object o: values) {
      if(!(o instanceof Float)){
        throw new IllegalArgumentException("Received non-float value" + o.toString() );
      }
      this.values[idx] = (Float)o;
      idx++;
    }
  }

  @Override
  public Float get(int index) {
    return values[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<Object> iterator() {
    return ((List)Floats.asList(values)).iterator();
  }
}
