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
package com.cloudera.exhibit.server.resources;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitId;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class FetchResponse {

  @JsonProperty
  public ExhibitId id;

  @JsonProperty
  public Exhibit exhibit;

  @JsonProperty
  public Map<String, Map<String, Object>> metrics;

  public FetchResponse(ExhibitId id, Exhibit exhibit, Map<String, Map<String, Object>> metrics) {
    this.id = id;
    this.exhibit = exhibit;
    this.metrics = metrics;
  }
}
