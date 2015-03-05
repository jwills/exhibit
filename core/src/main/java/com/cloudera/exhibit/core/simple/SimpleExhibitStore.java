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
import com.cloudera.exhibit.core.ExhibitId;
import com.cloudera.exhibit.core.ExhibitStore;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

public class SimpleExhibitStore implements ExhibitStore {

  private final String entity;
  private final Map<String, Exhibit> exhibits;

  public static SimpleExhibitStore of(String entity, Map<String, Exhibit> exhibits) {
    return new SimpleExhibitStore(entity, exhibits);
  }

  public SimpleExhibitStore(String entity, Map<String, Exhibit> exhibits) {
    this.entity = Preconditions.checkNotNull(entity);
    this.exhibits = Preconditions.checkNotNull(exhibits);
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public Set<String> entities() {
    return ImmutableSet.of(entity);
  }

  @Override
  public Optional<Exhibit> find(ExhibitId id) {
    if (!entity.equals(id.getEntity())) {
      return Optional.absent();
    }
    return Optional.fromNullable(exhibits.get(id.getId()));
  }
}
