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
package com.cloudera.exhibit.mongodb;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.bson.BSONObject;

import java.util.Iterator;
import java.util.List;

public class BSONFrame extends Frame {

  private BSONObsDescriptor descriptor;
  private List<? extends BSONObject> records;

  public BSONFrame(BSONObsDescriptor descriptor, List<? extends BSONObject> records) {
    this.descriptor = descriptor;
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
    return new BSONObs(descriptor, records.get(index));
  }

  @Override
  public Iterator<Obs> iterator() {
    return Iterators.transform(records.iterator(), new Function<BSONObject, Obs>() {
      @Override
      public Obs apply(BSONObject bsonObject) {
        return new BSONObs(descriptor, bsonObject);
      }
    });
  }
}
