/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.renjin;

import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.renjin.sexp.DoubleArrayVector;
import org.renjin.sexp.IntArrayVector;
import org.renjin.sexp.ListVector;
import org.renjin.sexp.LogicalArrayVector;
import org.renjin.sexp.StringArrayVector;
import org.renjin.sexp.Vector;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ListVectorFrame extends Frame {

  private Map<String, ObsDescriptor.FieldType> TYPE_NAMES = ImmutableMap.<String, ObsDescriptor.FieldType>builder()
      .put(LogicalArrayVector.TYPE_NAME, ObsDescriptor.FieldType.BOOLEAN)
      .put(IntArrayVector.TYPE_NAME, ObsDescriptor.FieldType.INTEGER)
      .put(DoubleArrayVector.TYPE_NAME, ObsDescriptor.FieldType.DOUBLE)
      .put(StringArrayVector.TYPE_NAME, ObsDescriptor.FieldType.STRING)
      .build();

  private ListVector res;
  private ObsDescriptor descriptor;

  public ListVectorFrame(ListVector res) {
    this.res = res;
    List<ObsDescriptor.Field> fields = Lists.newArrayListWithExpectedSize(res.length());
    for (int i = 0; i < res.length(); i++) {
      fields.add(new ObsDescriptor.Field(res.getName(i), TYPE_NAMES.get(res.get(i).getTypeName())));
    }
    this.descriptor = new SimpleObsDescriptor(fields);
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    //TODO
    return res.get(0).length();
  }

  @Override
  public Obs get(int rowIndex) {
    return new LVObs(rowIndex);
  }

  @Override
  public Iterator<Obs> iterator() {
    return new LVObsIterator();
  }

  @Override
  public String toString() {
    return res.toString();
  }

  public class LVObs extends Obs {

    private int row;

    public LVObs(int row) {
      this.row = row;
    }

    @Override
    public ObsDescriptor descriptor() {
      return descriptor;
    }

    @Override
    public Object get(int index) {
      return ((Vector) res.get(index)).getElementAsObject(row);
    }
  }

  public class LVObsIterator implements Iterator<Obs> {
    int offset = 0;

    @Override
    public boolean hasNext() {
      return offset < size();
    }

    @Override
    public Obs next() {
      Obs next = new LVObs(offset);
      offset++;
      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
