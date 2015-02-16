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
package com.cloudera.exhibit.server.checks;

import com.cloudera.exhibit.core.ExhibitStore;
import com.codahale.metrics.health.HealthCheck;

public class ExhibitStoreCheck extends HealthCheck {

  private final ExhibitStore store;

  public ExhibitStoreCheck(ExhibitStore store) {
    this.store = store;
  }

  @Override
  protected Result check() throws Exception {
    try {
      boolean connected = store.isConnected();
      if (connected) {
        return Result.healthy();
      } else {
        return Result.unhealthy("Disconnected");
      }
    } catch (Throwable t) {
      return Result.unhealthy(t);
    }
  }
}
