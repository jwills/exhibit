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
import com.google.common.primitives.Longs;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class LongVector extends Vector {

  private long [] values;
  private int size;

  protected LongVector(){
    this(Collections.emptyList());
  }

  // Construct which avoids the copy of data
  public LongVector(final long[] arr) {
    super(FieldType.LONG);
    size = arr.length;
    values = arr;
  }

  protected LongVector(List<Object> values) {
    super(FieldType.LONG);
    this.size = values.size();
    this.values = new long[this.size];
    int idx = 0;
    for(Object o: values) {
      if(!(o instanceof Long)){
        throw new IllegalArgumentException("Received non-long value" + o.toString() );
      }
      this.values[idx] = (Long)o;
      idx++;
    }
  }

  @Override
  public Long get(int index) {
    return values[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<Object> iterator() {
    return ((List)Longs.asList(values)).iterator();
  }
}
