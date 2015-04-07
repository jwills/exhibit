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
package com.cloudera.exhibit.sql;

import com.google.common.collect.Maps;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class ModifiableSchema extends AbstractSchema {

  private final Map<String, Table> tableMap;

  public ModifiableSchema() {
    this.tableMap = Maps.newHashMap();
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }

  public FrameTable getFrame(String name) {
    return (FrameTable) tableMap.get(name);
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public boolean contentsHaveChangedSince(long lastCheck, long now) {
    return true; // disable caching for now
  }
}
