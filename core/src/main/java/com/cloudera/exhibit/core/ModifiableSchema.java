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
package com.cloudera.exhibit.core;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractSchema;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

public class ModifiableSchema extends AbstractSchema {

  private final Map<String, Table> tableMap;
  private final Map<String, Table> tempTableMap;
  public ModifiableSchema() {
    this.tableMap = Maps.newHashMap();
    this.tempTableMap = Maps.newHashMap();
  }

  @Override
  public Map<String, Table> getTableMap() {
    return new AbstractMap<String, Table>() {
      @Override
      public Set<Entry<String, Table>> entrySet() {
        return Sets.union(tableMap.entrySet(), tempTableMap.entrySet());
      }

      @Override
      public Table get(Object name) {
        if (tableMap.containsKey(name)) {
          return tableMap.get(name);
        } else if (tempTableMap.containsKey(name)) {
          return tempTableMap.get(name);
        }
        return null;
      }
    };
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public boolean contentsHaveChangedSince(long lastCheck, long now) {
    return true; // disable caching for now
  }

  public void put(String name, Table tbl) {
    tableMap.put(name, tbl);
  }

  public void putTemp(String name, Table tbl) {
    this.tempTableMap.put(name, tbl);
  }

  public void clearTempTables() {
    this.tempTableMap.clear();
  }
}
