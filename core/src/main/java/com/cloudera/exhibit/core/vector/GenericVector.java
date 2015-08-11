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

