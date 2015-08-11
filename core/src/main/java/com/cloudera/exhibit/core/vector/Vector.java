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
import com.cloudera.exhibit.core.Vec;
import com.google.common.base.Objects;

public abstract class Vector implements Vec {
  private FieldType type;

  protected Vector(FieldType type){
    this.type = type;
  }

  public FieldType getType(){
    return this.type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Vector)) return false;
    Vector vector = (Vector) o;
    if(getType()!=vector.getType()){
      return false;
    }
    if(size()!=vector.size()){
      return false;
    }
    for (int i = 0; i < size(); i++) {
      if(!get(i).equals(vector.get(i))){
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getType()) + 17*size();
  }

}
