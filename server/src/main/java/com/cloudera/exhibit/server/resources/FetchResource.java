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
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/exhibit")
@Produces(MediaType.APPLICATION_JSON)
public class FetchResource {

  private static final Logger LOG = LoggerFactory.getLogger(FetchResource.class);

  private ExhibitStore store;

  public FetchResource(ExhibitStore store) {
    this.store = Preconditions.checkNotNull(store);
  }

  @GET
  public Optional<Exhibit> fetch(@QueryParam("id") String id) {
    Optional<Exhibit> res = store.find(id);
    return res;
  }
}
