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
package com.cloudera.exhibit.core.multi;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitId;
import com.cloudera.exhibit.core.ExhibitStore;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiExhibitStore implements ExhibitStore {

  private final Map<String, ExhibitStore> stores;

  public static MultiExhibitStore create(List<ExhibitStore> stores) {
    Map<String, ExhibitStore> storeMap = Maps.newHashMap();
    for (ExhibitStore store : stores) {
      for (String entity : store.entities()) {
        // TODO double check this
        storeMap.put(entity, store);
      }
    }
    return new MultiExhibitStore(storeMap);
  }

  public MultiExhibitStore(Map<String, ExhibitStore> stores) {
    this.stores = Preconditions.checkNotNull(stores);
  }

  @Override
  public boolean isConnected() {
    for (ExhibitStore store : stores.values()) {
      if (!store.isConnected()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<String> entities() {
    return stores.keySet();
  }

  @Override
  public Optional<Exhibit> find(ExhibitId id) {
    return stores.get(id.getEntity()).find(id);
  }
}
