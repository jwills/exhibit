package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;

import java.util.List;

public class VectorBuilder {

  public static Vector build(FieldType type, List<Object> values) {
    switch(type) {
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new DoubleVector(values);
      case BOOLEAN:
        return new BooleanVector(values);
      case SHORT:
      case INTEGER:
        return new IntVector(values);
      case STRING:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case DECIMAL:
        return new GenericVector(type, values);
    }
    throw new IllegalArgumentException("Unsupported FieldType: " + type);
  }

  public static Vector doubles(List<Object> values) {
    return new DoubleVector(values);
  }

  public static Vector bools(List<Object> values) {
    return new BooleanVector(values);
  }

  public static Vector shorts(List<Object> values) {
    return new IntVector(values);
    // return new ShortVector(values);
  }

  public static Vector ints(List<Object> values) {
    return new IntVector(values);
  }

  public static Vector longs(List<Object> values) {
    return new DoubleVector(values);
    // return new LongVector(values);
  }

  public static Vector floats(List<Object> values) {
    return new DoubleVector(values);
    // return new FloatVector(values);
  }
}
