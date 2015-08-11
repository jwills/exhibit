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
package com.cloudera.exhibit.core.simple;

import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class SimpleFrame extends Frame {

  private final ObsDescriptor descriptor;
  private final List<Obs> observations;

  public static SimpleFrame of(Iterable<Obs> obs) {
    return new SimpleFrame(obs);
  }
  public static SimpleFrame of(Obs... obs) {
    return new SimpleFrame(ImmutableList.copyOf(obs));
  }

  public SimpleFrame(Iterable<Obs> observations){
    this(Lists.newArrayList(observations));
  }

  public SimpleFrame(ObsDescriptor descriptor) {
    this(descriptor, ImmutableList.<Obs>of());
  }

  public SimpleFrame(List<Obs> observations) {
    this(observations.get(0).descriptor(), observations);
  }

  public SimpleFrame(ObsDescriptor descriptor, List<Obs> observations) {
    this.descriptor = descriptor;
    this.observations = observations;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    return observations.size();
  }

  @Override
  public Obs get(int index) {
    return observations.get(index);
  }

  @Override
  public Iterator<Obs> iterator() {
    return observations.iterator();
  }

  @Override
  public String toString() {
    return observations.toString();
  }
}
