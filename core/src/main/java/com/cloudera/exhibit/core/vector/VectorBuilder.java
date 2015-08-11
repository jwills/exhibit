/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.exhibit.core.vector;

import com.cloudera.exhibit.core.FieldType;

import java.util.Arrays;
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

  public static Vector bools(Boolean... b) {
    return bools(Arrays.<Object>asList(b));
  }
}
