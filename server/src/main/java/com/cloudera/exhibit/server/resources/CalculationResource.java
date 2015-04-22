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

import com.cloudera.exhibit.server.calcs.CalculationStore;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.IntParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/calculation")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CalculationResource {

  private CalculationStore calcs;

  public CalculationResource(CalculationStore calcs) {
    this.calcs = Preconditions.checkNotNull(calcs);
  }

  @GET
  public CalculationResponse getCode(@QueryParam("id") IntParam id) {
    return new CalculationResponse(calcs.getCode(id.get()));
  }

  @POST
  public Response save(SaveRequest request) {
    calcs.addCalculation(request.code);
    return Response.ok().build();
  }
}
