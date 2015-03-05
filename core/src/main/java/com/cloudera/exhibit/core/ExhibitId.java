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

import com.google.common.base.Preconditions;

public class ExhibitId {
  String entity;
  String id;

  public static ExhibitId create(String entity, Object id) {
    return new ExhibitId(entity, id.toString());
  }

  public ExhibitId(String entity, String id) {
    this.entity = Preconditions.checkNotNull(entity);
    this.id = Preconditions.checkNotNull(id);
  }

  public String getEntity() {
    return entity;
  }

  public String getId() {
    return id;
  }

  @Override
  public int hashCode() {
    return entity.hashCode() + 17 * id.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ExhibitId)) {
      return false;
    }
    ExhibitId eid = (ExhibitId) other;
    return entity.equals(eid.entity) && id.equals(eid.id);
  }

  @Override
  public String toString() {
    return new StringBuilder().append(entity).append(": ").append(id).toString();
  }
}
