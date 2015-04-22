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
package com.cloudera.exhibit.server.main;

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.cloudera.exhibit.core.simple.SimpleExhibitStore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.dropwizard.setup.Environment;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;

import javax.validation.Valid;
import java.util.Map;

public class ExhibitStoreConfig {
  @JsonProperty
  @Valid
  String name;

  @JsonProperty
  @Valid
  String uri;

  @JsonProperty
  @Valid
  String idColumn;

  public ExhibitStore create(Environment env, Configuration conf) {
    Dataset<GenericRecord> data = Datasets.load(uri);
    DatasetReader<GenericRecord> reader = data.newReader();
    Map<String, Exhibit> exhibits = Maps.newHashMap();
    try {
      while (reader.hasNext()) {
        GenericRecord rec = reader.next();
        Exhibit e = AvroExhibit.create(rec);
        exhibits.put(e.attributes().get(idColumn, String.class), e);
      }
    } finally {
      reader.close();
    }
    return SimpleExhibitStore.of(name, exhibits);
  }

  @Override
  public String toString() {
    return name;
  }
}
