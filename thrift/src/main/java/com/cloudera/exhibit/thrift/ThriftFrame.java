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
package com.cloudera.exhibit.thrift;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.thrift.TBase;

import java.util.Iterator;
import java.util.List;

public class ThriftFrame extends Frame {

  private final ThriftObsDescriptor descriptor;
  private final List<? extends TBase> records;

  public ThriftFrame(Class<? extends TBase> clazz) {
    this.descriptor = new ThriftObsDescriptor(clazz);
    this.records = ImmutableList.of();
  }

  public ThriftFrame(List<? extends TBase> records) {
    this.descriptor = new ThriftObsDescriptor(records.get(0).getClass());
    this.records = records;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    return records.size();
  }

  @Override
  public Obs get(int index) {
    return new ThriftObs(descriptor, records.get(index));
  }

  @Override
  public Iterator<Obs> iterator() {
    return Iterators.transform(records.iterator(), new Function<TBase, Obs>() {
      @Override
      public Obs apply(TBase tBase) {
        return new ThriftObs(descriptor, tBase);
      }
    });
  }
}
