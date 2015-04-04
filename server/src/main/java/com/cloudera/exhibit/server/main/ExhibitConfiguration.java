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

import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.core.multi.MultiExhibitStore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import java.util.List;

public class ExhibitConfiguration extends Configuration {

  private static final Logger LOG = LoggerFactory.getLogger(ExhibitConfiguration.class);

  @JsonProperty
  @Valid
  List<ExhibitStoreConfig> exhibits;

  public ExhibitStore getExhibitStores(final Environment env, final org.apache.hadoop.conf.Configuration conf) {
    return MultiExhibitStore.create(Lists.transform(exhibits, new Function<ExhibitStoreConfig, ExhibitStore>() {
      @Override
      public ExhibitStore apply(ExhibitStoreConfig exhibitStoreConfig) {
        LOG.info("Creating exhibit store from config: " + exhibitStoreConfig);
        return exhibitStoreConfig.create(env, conf);
      }
    }));
  }
}
