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
package com.cloudera.exhibit.core;

import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;

public abstract class Frame implements Iterable<Obs>, Serializable {
  public abstract ObsDescriptor descriptor();
  public abstract int size();
  public abstract Obs get(int rowIndex);

  public Column $(int columnIndex) {
    return Column.create(this, columnIndex);
  }

  public Column $(String columnName) {
    return Column.create(this, columnName);
  }

  @Override
  public int hashCode(){
    return  descriptor().hashCode()
         +  3 * Objects.hash(Iterables.toArray((Iterable<? extends Obs>) iterator(), Obs.class));
  }


  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Frame )) {
      return false;
    }
    Frame otherFrame = (Frame)other;
    if(size()!=otherFrame.size()){
      return false;
    }
    if(!descriptor().equals(otherFrame.descriptor())) {
      return false;
    }
    Iterator<Obs> thisIter  = iterator();
    Iterator<Obs> otherIter = otherFrame.iterator();
    while(thisIter.hasNext() && otherIter.hasNext()){
      Obs thisObs  = thisIter.next();
      Obs otherObs = otherIter.next();
      if(!thisObs.equals(otherObs)){
        return false;
      }
    }
    return thisIter.hasNext() == otherIter.hasNext();
  }
}
