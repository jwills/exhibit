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
package com.cloudera.exhibit.core.composite;

import com.cloudera.exhibit.core.Column;
import com.cloudera.exhibit.core.ExhibitId;
import com.cloudera.exhibit.core.Frame;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class NeighborLookup {
  private final Map<String, String> columnToEntity;

  public static NeighborLookup create(String column, String entity, String... args) {
    Map<String, String> cte = Maps.newHashMap();
    cte.put(column, entity);
    for (int i = 0; i < args.length; i += 2) {
      cte.put(args[i], args[i + 1]);
    }
    return new NeighborLookup(cte);
  }

  public NeighborLookup(Map<String, String> columnToEntity) {
    this.columnToEntity = columnToEntity;
  }

  public Set<ExhibitId> lookupIds(Frame frame) {
    Set<ExhibitId> ids = Sets.newHashSet();
    for (Map.Entry<String, String> e : columnToEntity.entrySet()) {
      Column column = Column.create(frame, e.getKey());
      for (Object value : column) {
        ids.add(ExhibitId.create(e.getValue(), value));
      }
    }
    return ids;
  }
}
