package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;
import com.google.common.primitives.Doubles;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DoubleVector extends Vector {

  private double [] values;
  private int size;

  protected DoubleVector(){
    this(Collections.emptyList());
  }

  // Construct which avoids the copy of data
  public DoubleVector(final double[] arr) {
    super(FieldType.DOUBLE);
    size = arr.length;
    values = arr;
  }

  protected DoubleVector(List<Object> values) {
    super(FieldType.DOUBLE);
    this.size = values.size();
    this.values = new double[this.size];
    int idx = 0;
    for(Object o: values) {
      if(!(o instanceof Double)){
        throw new IllegalArgumentException("Received non-double value" + o.toString() );
      }
      this.values[idx] = (Double)o;
      idx++;
    }
  }

  @Override
  public Double get(int index) {
    return values[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<Object> iterator() {
    return ((List)Doubles.asList(values)).iterator();
  }
}
