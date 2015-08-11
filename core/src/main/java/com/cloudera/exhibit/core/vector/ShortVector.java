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
import com.google.common.primitives.Shorts;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ShortVector extends Vector {

  private short [] values;
  private int size;

  protected ShortVector(){
    this(Collections.emptyList());
  }

  // Construct which avoids the copy of data
  public ShortVector(final short[] arr) {
    super(FieldType.SHORT);
    size = arr.length;
    values = arr;
  }

  protected ShortVector(List<Object> values) {
    super(FieldType.SHORT);
    this.size = values.size();
    this.values = new short[this.size];
    int idx = 0;
    for(Object o: values) {
      if(!(o instanceof Short)){
        throw new IllegalArgumentException("Received non-short value" + o.toString() + " class: " + o.getClass().getCanonicalName());
      }
      this.values[idx] = (Short)o;
      idx++;
    }
  }

  @Override
  public Short get(int index) {
    return values[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<Object> iterator() {
    return ((List)Shorts.asList(values)).iterator();
  }
}
