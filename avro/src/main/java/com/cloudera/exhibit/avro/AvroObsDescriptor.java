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
package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.List;

public class AvroObsDescriptor implements ObsDescriptor {

  private static Schema NULL = Schema.create(Schema.Type.NULL);

  private final Schema schema;

  public AvroObsDescriptor(Schema schema) {
    this.schema = unwrap(schema);
  }

  @Override
  public Field get(int index) {
    Schema.Field f = schema.getFields().get(index);
    return new Field(f.name(), getFieldType(f.schema()));
  }

  @Override
  public int indexOf(String name) {
    Schema.Field f = schema.getField(name);
    return f == null ? -1 : f.pos();
  }

  static Schema unwrap(Schema s) {
    if (s.getType() == Schema.Type.UNION) {
      List<Schema> ut = s.getTypes();
      if (NULL.equals(ut.get(0))) {
        return unwrap(ut.get(1));
      } else if (NULL.equals(ut.get(1))) {
        return unwrap(ut.get(0));
      }
    }
    return s;
  }

  static FieldType getFieldType(Schema s) {
    s = unwrap(s);
    switch (s.getType()) {
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case INT:
        return FieldType.INTEGER;
      case LONG:
        return FieldType.LONG;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
        return FieldType.DOUBLE;
      case STRING:
        return FieldType.STRING;
      default:
        System.err.println("Unknown schema type = " + s);
        return null;
    }
  }

  @Override
  public int size() {
    return schema.getFields().size();
  }

  @Override
  public Iterator<Field> iterator() {
    return Iterators.transform(schema.getFields().iterator(), new Function<Schema.Field, Field>() {
      @Override
      public Field apply(Schema.Field f) {
        return new Field(f.name(), getFieldType(f.schema()));
      }
    });
  }
}
