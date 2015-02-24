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
package com.cloudera.exhibit.server.store;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public class TestExhibitStore implements ExhibitStore {
  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public Optional<Exhibit> find(String id) {
    Exhibit e = new SimpleExhibit(Obs.EMPTY, ImmutableMap.<String, Frame>of());
    return Optional.of(e);
  }
}
