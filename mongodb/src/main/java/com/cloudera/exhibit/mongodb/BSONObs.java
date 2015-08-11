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

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import org.bson.BSONObject;

public class BSONObs extends Obs {

  private BSONObsDescriptor descriptor;
  private BSONObject bson;

  public BSONObs(BSONObsDescriptor descriptor, BSONObject bson) {
    this.descriptor = descriptor;
    this.bson = bson;
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    return descriptor().size();
  }

  @Override
  public Object get(int index) {
    return bson.get(descriptor.getBSONColumn(index));
  }
}
