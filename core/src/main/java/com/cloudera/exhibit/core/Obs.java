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

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Iterator;

public abstract class Obs implements Iterable<Object>, Serializable {
  public abstract ObsDescriptor descriptor();

  public abstract Object get(int index);

  public Object get(String name) {
    return get(descriptor().indexOf(name));
  }

  public <T> T get(String name, Class<T> clazz) {
    return clazz.cast(get(name));
  }

  public Iterator<Object> iterator() {
    return new Iterator<Object>() {
      int offset = 0;
      @Override
      public boolean hasNext() {
        return offset < descriptor().size();
      }

      @Override
      public Object next() {
        Object ret = get(offset);
        offset++;
        return ret;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
  public static final Obs EMPTY = new Obs() {
    @Override
    public Iterator<Object> iterator() {
      return ImmutableList.of().iterator();
    }

    @Override
    public ObsDescriptor descriptor() {
      return ObsDescriptor.EMPTY;
    }

    @Override
    public Object get(int index) {
      throw new ArrayIndexOutOfBoundsException("Empty Obs");
    }
  };

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    if (descriptor().size() > 0) {
      sb.append(get(0));
      for (int i = 1; i < descriptor().size(); i++) {
        sb.append(',').append(get(i));
      }
    }
    return sb.append(']').toString();
  }
}
