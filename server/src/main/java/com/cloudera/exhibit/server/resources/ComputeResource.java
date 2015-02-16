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
package com.cloudera.exhibit.server.resources;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.base.Preconditions;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.sql.ResultSet;
import java.sql.SQLException;

@Path("/compute")
@Produces(MediaType.TEXT_PLAIN)
public class ComputeResource {

  private final ExhibitStore store;

  public ComputeResource(ExhibitStore store) {
    this.store = Preconditions.checkNotNull(store);
  }

  @POST
  public String compute(@FormParam("id") String id, @FormParam("code") String code) throws SQLException {
    Exhibit exhibit = store.find(id).orNull();
    String[] queries = code.split(";");
    SQLCalculator calc = new SQLCalculator(queries);
    ResultSet rs = calc.apply(exhibit);
    return rs.getString(1); // TODO: fix this
  }

}
