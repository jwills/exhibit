/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

public class SimpleExhibitStore implements ExhibitStore {

  private final Map<String, Exhibit> exhibits;

  public static SimpleExhibitStore of(String id, Exhibit e, Object... rest) {
    Map<String, Exhibit> exhibits = Maps.newHashMap();
    exhibits.put(id, e);
    for (int i = 0; i < rest.length; i += 2) {
      exhibits.put(rest[i].toString(), (Exhibit) rest[i + 1]);
    }
    return new SimpleExhibitStore(exhibits);
  }

  public SimpleExhibitStore(Map<String, Exhibit> exhibits) {
    this.exhibits = Preconditions.checkNotNull(exhibits);
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public Optional<Exhibit> find(String id) {
    return Optional.fromNullable(exhibits.get(id));
  }
}
