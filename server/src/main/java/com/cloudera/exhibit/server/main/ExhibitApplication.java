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
package com.cloudera.exhibit.server.main;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibitStore;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.server.checks.ExhibitStoreCheck;
import com.cloudera.exhibit.server.json.ExhibitSerializer;
import com.cloudera.exhibit.server.resources.ComputeResource;
import com.cloudera.exhibit.server.resources.FetchResource;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dropwizard.Application;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.skife.jdbi.v2.DBI;

public class ExhibitApplication extends Application<ExhibitConfiguration> {

  public static void main(String[] args) throws Exception {
    new ExhibitApplication().run(args);
  }

  @Override
  public void initialize(Bootstrap<ExhibitConfiguration> bootstrap) {

  }

  @Override
  public void run(ExhibitConfiguration conf, Environment env) throws Exception {
    // basic env stuff
    setupMapper(env.getObjectMapper());
    ExhibitStore store = getStore(conf, env);

    // health checks
    env.healthChecks().register("store", new ExhibitStoreCheck(store));

    // resources
    final FetchResource fetch = new FetchResource(store);
    env.jersey().register(fetch);
    final ComputeResource compute = new ComputeResource(store);
    env.jersey().register(compute);
  }

  ExhibitStore getStore(ExhibitConfiguration config, Environment environment) throws ClassNotFoundException {
    final DBIFactory factory = new DBIFactory();
    final DBI jdbi = factory.build(environment, config.getDataSourceFactory(), "exhibit");
    ObsDescriptor od = SimpleObsDescriptor.of("a", ObsDescriptor.FieldType.INTEGER, "b", ObsDescriptor.FieldType.DOUBLE,
            "c", ObsDescriptor.FieldType.STRING);
    SimpleObs o1 = SimpleObs.of(od, 17, 29.0, "josh");
    SimpleObs o2 = SimpleObs.of(od, 29, 1729.0, "wills");
    SimpleObs o3 = SimpleObs.of(od, null, null, "some other name");
    Frame f = SimpleFrame.of(o1, o2, o3);
    Exhibit e = SimpleExhibit.of("data", f);
    return SimpleExhibitStore.of("josh", e);
  }

  void setupMapper(ObjectMapper mapper) {
    SimpleModule mod = new SimpleModule("exhibit", Version.unknownVersion());
    mod.addSerializer(Exhibit.class, new ExhibitSerializer());
    mapper.registerModule(mod);
  }
}
