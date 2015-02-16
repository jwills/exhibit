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
package com.cloudera.exhibit.server.jdbi;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.Iterator;
import java.util.Map;

public class JdbiExhibitStore implements ExhibitStore {

  private Map<String, Exhibit> exhibits;

  public static JdbiExhibitStore create(DBI jdbi, String table, String id) {
    Handle handle = jdbi.open();
    Iterator<Exhibit> iter = handle.createQuery("select * from " + table)
            .map(new ExhibitMapper(id))
            .iterator();
    Map<String, Exhibit> exhibits = Maps.newHashMap();
    while (iter.hasNext()) {
      Exhibit e = iter.next();
      exhibits.put(e.attributes().get(id, String.class), e);
    }
    return new JdbiExhibitStore(exhibits);
  }

  private JdbiExhibitStore(Map<String, Exhibit> exhibits) {
    this.exhibits = exhibits;
  }

  @Override
  public boolean isConnected() {
    return true; // TODO just caching everything for now
  }

  @Override
  public Optional<Exhibit> find(String id) {
    return Optional.fromNullable(exhibits.get(id));
  }
}
