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

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.ObsDescriptor.Field;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AvroExhibitTest {

  static Schema createNullableType(Schema s) {
    return Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL),s));
  }

  Schema inner = SchemaBuilder.record("foo").fields()
          .nullableString("f1", "")
          .optionalBoolean("f2")
          .requiredInt("f3")
          .endRecord();

  Schema outer1 = SchemaBuilder.record("bar").fields()
          .requiredInt("id")
          .name("ifoo").type(createNullableType(Schema.createArray(inner))).noDefault()
          .optionalString("short")
          .endRecord();

  Schema outer2 = SchemaBuilder.record("bar").fields()
          .requiredInt("id")
          .name("ifoo").type(createNullableType(
                Schema.createArray(createNullableType(inner)))).noDefault()
          .optionalString("short")
          .endRecord();

  @Test
  public void testExhibitDescriptorFrame() {
    // Apply same tests to both schemas
    // null-able inner types should not alter inference
    List<ExhibitDescriptor> descriptors = ImmutableList.of(
        AvroExhibit.createDescriptor(outer1)
        , AvroExhibit.createDescriptor(outer2));
    for(ExhibitDescriptor desc: descriptors){
      ObsDescriptor attributes = desc.attributes();
      assertEquals("Infer number of attributes", 2, attributes.size());
      assertEquals(new Field("id", FieldType.INTEGER), attributes.get(0));
      assertEquals(new Field("short", FieldType.STRING), attributes.get(1));
      Map<String, ObsDescriptor> frames = desc.frames();
      assertEquals("Infer number of frames", 1, frames.size());
      ObsDescriptor frameDesc = frames.get("ifoo");
      assertNotNull("Retrieve `ifoo` Frame", frameDesc);
      assertEquals("Infer Frame Structure", 3, frameDesc.size());
      assertEquals(new Field("f1", FieldType.STRING), frameDesc.get(frameDesc.indexOf("f1")));
      assertEquals(new Field("f2", FieldType.BOOLEAN), frameDesc.get(frameDesc.indexOf("f2")));
      assertEquals(new Field("f3", FieldType.INTEGER), frameDesc.get(frameDesc.indexOf("f3")));
      Map<String, FieldType> vectors = desc.vectors();
      assertEquals("Infer number of vectors", 0, vectors.size());
    }
  }

  @Test
  public void testExhibitDescriptorVector(){
    Schema simpleVectorRecordSchema = SchemaBuilder.record("rec").fields()
        .requiredInt("id")
        .name("vec").type(Schema.createArray(Schema.create(Schema.Type.DOUBLE))).noDefault()
        .endRecord();
    ExhibitDescriptor desc = AvroExhibit.createDescriptor(simpleVectorRecordSchema);
    ObsDescriptor attributes = desc.attributes();
    assertEquals("Infer number of attributes", 1, attributes.size());
    assertEquals(new Field("id", FieldType.INTEGER), attributes.get(0));
    Map<String, ObsDescriptor> frames = desc.frames();
    assertEquals("Infer number of frames", 0, frames.size());
    Map<String, FieldType> vectors = desc.vectors();
    assertEquals("Infer number of vectors", 1, vectors.size());
    FieldType vectorDesc = vectors.get("vec");
    assertEquals(FieldType.DOUBLE, vectorDesc);
  }

  @Test
  public void testAvroExhibitFrame() {
    GenericData.Record in1 = new GenericData.Record(inner);
    in1.put("f1", null);
    in1.put("f2", true);
    in1.put("f3", 1729);
    GenericData.Record in2 = new GenericData.Record(inner);
    in2.put("f1", "josh");
    in2.put("f3", 29);
    List<GenericData.Record> recs = Lists.newArrayList(in1, in2);
    GenericData.Record o1 = new GenericData.Record(outer1);
    o1.put("id", 17);
    o1.put("short", "jw");
    o1.put("ifoo", recs);

    Exhibit exhibit = AvroExhibit.create(o1);
    assertEquals(17, exhibit.attributes().get("id"));
    assertEquals("jw", exhibit.attributes().get("short"));
    Frame f = exhibit.frames().get("ifoo");
    assertNotNull(f);
    assertEquals("Num obs in Frame", 2, f.size());
    assertEquals(null, f.$("f1").get(0));
    assertEquals("josh", f.$("f1").get(1));
    assertEquals(true, f.$("f2").get(0));
    assertEquals(null, f.$("f2").get(1));
    assertEquals(1729, f.$("f3").get(0));
    assertEquals(29, f.$("f3").get(1));
  }

  @Test
  public void testAvroExhibitVector() {
    Schema simpleVectorRecordSchema = SchemaBuilder.record("rec").fields()
        .requiredInt("id")
        .name("vec").type(Schema.createArray(Schema.create(Schema.Type.DOUBLE))).noDefault()
        .endRecord();

    GenericData.Record o1 = new GenericData.Record(simpleVectorRecordSchema);
    o1.put("id", 123);
    o1.put("vec", ImmutableList.of(1.0, 2.0, 3.0).toArray());

    Exhibit exhibit = AvroExhibit.create(o1);
    assertEquals(123, exhibit.attributes().get("id"));
    Frame f = exhibit.frames().get("ifoo");
    assertNull(f);
    Vector v = exhibit.vectors().get("vec");
    assertNotNull(v);
    assertEquals("Num obs in Vector", 3, v.size());
    assertEquals(1.0, v.get(0));
    assertEquals(2.0, v.get(1));
    assertEquals(3.0, v.get(2));
  }
}
