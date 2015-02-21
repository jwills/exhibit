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
import com.cloudera.exhibit.server.checks.ExhibitStoreCheck;
import com.cloudera.exhibit.server.json.ExhibitSerializer;
import com.cloudera.exhibit.server.kite.KiteExhibitStore;
import com.cloudera.exhibit.server.resources.ComputeResource;
import com.cloudera.exhibit.server.resources.FetchResource;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dropwizard.Application;
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
  }

  @Override
  public void run(ExhibitConfiguration config, Environment env) throws Exception {
    // basic env stuff
    setupMapper(env.getObjectMapper());
    ExhibitStore store = getStore(config, env);

    // health checks
    env.healthChecks().register("store", new ExhibitStoreCheck(store));

    // resources
    final FetchResource fetch = new FetchResource(store);
    env.jersey().register(fetch);
    final ComputeResource compute = new ComputeResource(store);
    env.jersey().register(compute);
  }

  ExhibitStore getStore(ExhibitConfiguration config, Environment environment) throws ClassNotFoundException {
    return KiteExhibitStore.create(conf, config.getExhibitDatabase(), config.getExhibitTable(), config.getExhibitIdColumn());
  }

  void setupMapper(ObjectMapper mapper) {
    SimpleModule mod = new SimpleModule("exhibit", Version.unknownVersion());
    mod.addSerializer(Exhibit.class, new ExhibitSerializer());
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
