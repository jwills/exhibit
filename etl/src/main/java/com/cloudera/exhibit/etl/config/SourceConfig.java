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
package com.cloudera.exhibit.etl.config;

import com.google.common.collect.Sets;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.Set;

public class SourceConfig implements Serializable {
  public String name;

  String schemaJson;

  transient Schema schema;

  public String uri;

  public boolean embedded = false;

  public boolean repeated = true;

  public boolean nullable = true;

  public Set<String> keyFields;

  public Set<String> invalidKeys = Sets.newHashSet();

  public void setSchema(Schema schema) {
    this.schemaJson = schema.toString();
  }

  public Schema getSchema() {
    if (schema == null) {
      schema = (new Schema.Parser()).parse(schemaJson);
    }
    return schema;
  }
}
