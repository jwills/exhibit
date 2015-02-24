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

import com.cloudera.exhibit.core.ExhibitStore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.conf.Configuration;

import javax.validation.Valid;

public class ExhibitStoreFactory {

  @JsonProperty
  @Valid
  boolean test = false;


  @JsonProperty
  @Valid
  String database = "";


  @JsonProperty
  @Valid
  String table = "";


  @JsonProperty
  @Valid
  String idColumn = "";

  public ExhibitStore build(Environment env, Configuration conf) {
    if (test) {
      return new TestExhibitStore();
    } else {
      return KiteExhibitStore.create(conf, database, table, idColumn);
    }
  }
}
