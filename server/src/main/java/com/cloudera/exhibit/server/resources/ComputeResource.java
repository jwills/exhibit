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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.base.Preconditions;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;

@Path("/compute")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ComputeResource {

  private final ExhibitStore store;

  public ComputeResource(ExhibitStore store) {
    this.store = Preconditions.checkNotNull(store);
  }

  @POST
  public Frame compute(@Valid ComputeRequest req) throws SQLException {
    Exhibit exhibit = store.find(req.id).orNull();
    SQLCalculator calc = SQLCalculator.create(null, req.code); //TODO
    calc.initialize(exhibit.descriptor());
    return calc.apply(exhibit);
  }

}
