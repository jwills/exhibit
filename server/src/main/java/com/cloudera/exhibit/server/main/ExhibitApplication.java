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
import com.cloudera.exhibit.server.json.ResultSetSerializer;
import com.cloudera.exhibit.server.store.KiteExhibitStore;
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

import java.sql.ResultSet;

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
    ExhibitStore store = config.getExhibitStoreFactory().build(env, getConf());

    // health checks
    env.healthChecks().register("store", new ExhibitStoreCheck(store));

    // API home
    env.jersey().setUrlPattern("/api/*");
    // resources
    final FetchResource fetch = new FetchResource(store);
    env.jersey().register(fetch);
    final ComputeResource compute = new ComputeResource(store);
    env.jersey().register(compute);
  }

  void setupMapper(ObjectMapper mapper) {
    SimpleModule mod = new SimpleModule("exhibit", Version.unknownVersion());
    mod.addSerializer(Exhibit.class, new ExhibitSerializer());
    mod.addSerializer(ResultSet.class, new ResultSetSerializer());
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
