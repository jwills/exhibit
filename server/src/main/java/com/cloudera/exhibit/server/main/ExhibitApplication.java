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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.ExhibitId;
import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.server.calcs.CalculationStore;
import com.cloudera.exhibit.server.checks.ExhibitStoresCheck;
import com.cloudera.exhibit.server.json.ExhibitIdDeserializer;
import com.cloudera.exhibit.server.json.ExhibitIdSerializer;
import com.cloudera.exhibit.server.json.ExhibitSerializer;
import com.cloudera.exhibit.server.json.FrameSerializer;
import com.cloudera.exhibit.server.resources.CalculationResource;
import com.cloudera.exhibit.server.resources.ComputeResource;
import com.cloudera.exhibit.server.resources.FetchResource;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class ExhibitApplication extends Application<ExhibitConfiguration> implements Configurable {

  private Configuration conf;

  public static void main(String[] args) throws Exception {
    ExhibitApplication app = new ExhibitApplication();
    Configuration conf = new Configuration();
    app.setConf(conf); //TODO: get settings from command line or exhibitconfig
    app.run(args);
  }

  @Override
  public void initialize(Bootstrap<ExhibitConfiguration> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
  }

  @Override
  public void run(ExhibitConfiguration config, Environment env) throws Exception {
    // basic env stuff
    setupMapper(env.getObjectMapper());
    ExhibitStore exhibits = config.getExhibitStores(env, getConf());
    CalculationStore calcs = new CalculationStore();

    // health checks
    env.healthChecks().register("stores", new ExhibitStoresCheck(exhibits));

    // API home
    env.jersey().setUrlPattern("/api/*");
    // resources
    final FetchResource fetch = new FetchResource(exhibits, calcs);
    env.jersey().register(fetch);
    final ComputeResource compute = new ComputeResource(exhibits);
    env.jersey().register(compute);
    final CalculationResource calculations = new CalculationResource(calcs);
    env.jersey().register(calculations);
  }

  void setupMapper(ObjectMapper mapper) {
    SimpleModule mod = new SimpleModule("exhibit", Version.unknownVersion());
    mod.addSerializer(ExhibitId.class, new ExhibitIdSerializer());
    mod.addDeserializer(ExhibitId.class, new ExhibitIdDeserializer());
    mod.addSerializer(Exhibit.class, new ExhibitSerializer());
    mod.addSerializer(Frame.class, new FrameSerializer());
    mapper.registerModule(mod);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
