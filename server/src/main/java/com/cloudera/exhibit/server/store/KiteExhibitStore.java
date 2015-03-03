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
package com.cloudera.exhibit.server.store;

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitStore;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;

import java.util.Map;

public class KiteExhibitStore implements ExhibitStore {

  private final Map<String, Exhibit> exhibits;

  public static ExhibitStore create(String uri, String idCol) {
    Dataset<GenericRecord> data = Datasets.load(uri);
    DatasetReader<GenericRecord> reader = data.newReader();
    Map<String, Exhibit> exhibits = Maps.newHashMap();
    try {
      while (reader.hasNext()) {
        GenericRecord rec = reader.next();
        Exhibit e = AvroExhibit.create(rec);
        exhibits.put(e.attributes().get(idCol, String.class), e);
      }
    } finally {
      reader.close();
    }
    return new KiteExhibitStore(exhibits);
  }

  private KiteExhibitStore(Map<String, Exhibit> exhibits) {
    this.exhibits = exhibits;
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public Optional<Exhibit> find(String id) {
    return Optional.fromNullable(exhibits.get(id));
  }


}
