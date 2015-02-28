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
package com.cloudera.exhibit.etl;

import org.apache.avro.Schema;
import org.apache.crunch.Target;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import java.util.List;

public class ExhibitToolConfig {

  public static enum KeyType {
    STRING {
      @Override
      public PType<?> getPType() {
        return Avros.strings();
      }

      @Override
      public Schema getSchema() {
        return Schema.create(Schema.Type.STRING);
      }
    },

    LONG {
      @Override
      public PType<?> getPType() {
        return Avros.longs();
      }

      @Override
      public Schema getSchema() {
        return Schema.create(Schema.Type.LONG);
      }
    };

    public abstract PType<?> getPType();

    public abstract Schema getSchema();
  }

  public String database;

  public String table;

  public String keyField;

  public KeyType keyType;

  public Target.WriteMode writeMode = Target.WriteMode.OVERWRITE;

  public int parallelism = -1;

  public List<SourceConfig> sources;
}
