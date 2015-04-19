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
package com.cloudera.exhibit.etl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SchemaProvider implements Serializable {
  private final List<String> json;
  private transient List<Schema> schemas;

  public SchemaProvider(List<Schema> schemas) {
    this.schemas = schemas;
    this.json = Lists.newArrayList(Lists.transform(schemas, new Function<Schema, String>() {
      @Nullable
      @Override
      public String apply(Schema schema) {
        return schema.toString();
      }
    }));
  }

  public Schema get(int i) {
   return getSchemas().get(i);
  }

  private List<Schema> getSchemas() {
    if (schemas == null) {
      final Schema.Parser sp = new Schema.Parser();
      Map<String, Schema> defined = Maps.newHashMap();
      this.schemas = Lists.newArrayList();
      for (String s : json) {
        if (defined.containsKey(s)) {
          schemas.add(defined.get(s));
        } else {
          Schema schema = sp.parse(s);
          defined.put(s, schema);
          schemas.add(schema);
        }
      }
    }
    return schemas;
  }
}
