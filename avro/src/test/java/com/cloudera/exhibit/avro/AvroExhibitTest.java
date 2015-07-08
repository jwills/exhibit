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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.util.List;

public class AvroExhibitTest {

  static Schema createNullableType(Schema s) {
    return Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL),s));
  }

  Schema inner = SchemaBuilder.record("foo").fields()
          .nullableString("f1", "")
          .optionalBoolean("f2")
          .requiredInt("f3")
          .endRecord();

  Schema outer = SchemaBuilder.record("bar").fields()
          .requiredInt("id")
          .name("ifoo").type(createNullableType(Schema.createArray(inner))).noDefault()
          .optionalString("short")
          .endRecord();

  Schema nullableInner = SchemaBuilder.record("bar").fields()
          .requiredInt("id")
          .name("ifoo").type(createNullableType(
                Schema.createArray(createNullableType(inner)))).noDefault()
          .optionalString("short")
          .endRecord();

  @Test
  public void testExhibitDescriptor() {
    ExhibitDescriptor desc = AvroExhibit.createDescriptor(outer);
    System.out.println(desc);
  }

  @Test
  public void testAvroExhibit() {
    GenericData.Record in1 = new GenericData.Record(inner);
    in1.put("f1", null);
    in1.put("f2", true);
    in1.put("f3", 1729);
    GenericData.Record in2 = new GenericData.Record(inner);
    in2.put("f1", "josh");
    in2.put("f3", 29);
    List<GenericData.Record> recs = Lists.newArrayList(in1, in2);

    GenericData.Record o1 = new GenericData.Record(outer);
    o1.put("id", 17);
    o1.put("short", "jw");
    o1.put("ifoo", recs);

    Exhibit exhibit = AvroExhibit.create(o1);
    System.out.println(exhibit);
    System.out.println(exhibit.attributes());
    System.out.println(Iterables.toString(exhibit.frames().get("ifoo")));
  }

  @Test
  public void testNullableExhibitDescriptor() {
    ExhibitDescriptor desc = AvroExhibit.createDescriptor(nullableInner);
    System.out.println(desc);
  }

  @Test
  public void testNullableElementArrayAvroExhibit() {
    GenericData.Record in1 = new GenericData.Record(inner);
    in1.put("f1", null);
    in1.put("f2", true);
    in1.put("f3", 1729);
    GenericData.Record in2 = new GenericData.Record(inner);
    in2.put("f1", "josh");
    in2.put("f3", 29);
    List<GenericData.Record> recs = Lists.newArrayList(in1, in2);

    GenericData.Record o1 = new GenericData.Record(nullableInner);
    o1.put("id", 17);
    o1.put("short", "jw");
    o1.put("ifoo", recs);

    Exhibit exhibit = AvroExhibit.create(o1);
    System.out.println(exhibit);
    System.out.println(exhibit.attributes());
    System.out.println(Iterables.toString(exhibit.frames().get("ifoo")));
  }
}
