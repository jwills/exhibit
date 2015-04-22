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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Set;

public class SchemaUtil {

  public static Schema getOrParse(Schema s, String json) {
    if (s == null) {
      s = (new Schema.Parser()).parse(json);
    }
    return s;
  }

  public static Schema unwrapNull(Schema s) {
    if (s.getType() == Schema.Type.UNION) {
      List<Schema> cmps = s.getTypes();
      if (cmps.size() == 2) {
        if (cmps.get(0).getType() == Schema.Type.NULL) {
          return cmps.get(1);
        } else if (cmps.get(1).getType() == Schema.Type.NULL) {
          return cmps.get(0);
        }
      }
    }
    return s;
  }

  public static Schema unionKeySchema(String name, List<Schema> schemas) {
    Schema wrapper = Schema.createRecord(name, "exhibit", "", false);
    Schema unionSchema = Schema.createUnion(schemas);
    Schema.Field idx = new Schema.Field("index", Schema.create(Schema.Type.INT), "", null);
    Schema.Field key = new Schema.Field("key", unionSchema, "", null);
    wrapper.setFields(Lists.newArrayList(idx, key));
    return wrapper;
  }

  public static Schema unionValueSchema(String name, List<Schema> schemas) {
    Schema wrapper = Schema.createRecord(name, "exhibit", "", false);
    Schema unionSchema = Schema.createUnion(schemas);
    Schema.Field sf = new Schema.Field("value", unionSchema, "", null);
    wrapper.setFields(Lists.newArrayList(sf));
    return wrapper;
  }

  private SchemaUtil() {}
}
