/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.etl.tbl;

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import org.apache.avro.generic.GenericData;

import java.util.Map;

public class TopTbl implements Tbl {

  private final Map<String, String> values;
  private final Map<String, Object> options;

  public TopTbl(Map<String, String> values, Map<String, Object> options) {
    this.values = values;
    this.options = options;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    return null;
  }

  @Override
  public void initialize(SchemaProvider provider) {

  }

  @Override
  public void add(Obs obs) {

  }

  @Override
  public GenericData.Record getValue() {
    return null;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    return null;
  }

  @Override
  public GenericData.Record finalize(GenericData.Record value) {
    return null;
  }
}
