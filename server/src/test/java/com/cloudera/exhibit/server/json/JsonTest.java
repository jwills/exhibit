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
package com.cloudera.exhibit.server.json;

import static org.junit.Assert.assertEquals;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.exhibit.avro.AvroFrame;
import com.cloudera.exhibit.avro.AvroObsDescriptor;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitId;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.mongodb.BSONFrame;
import com.cloudera.exhibit.mongodb.BSONObsDescriptor;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;

public class JsonTest {

  Schema schema = SchemaBuilder.record("foo").fields()
          .optionalInt("a")
          .optionalDouble("b")
          .optionalString("c")
          .endRecord();

  ObjectMapper mapper;

  @SuppressWarnings("unused")
  private String expected = "{\"attrs\":{}," +
          "\"columns\":{\"t1\":[\"a\",\"b\",\"c\"],\"e\":[\"a\",\"b\",\"c\"]}," +
          "\"frames\":{\"t1\":[[1729,null,\"foo\"],[1729,3.0,null],[null,17.0,null]],\"e\":[]}}";
  private String expected2 = "{\"attrs\":{}," +
          "\"columns\":{\"e\":[\"a\",\"b\",\"c\"],\"t1\":[\"a\",\"b\",\"c\"]}," +
          "\"frames\":{\"e\":[],\"t1\":[[1729,null,\"foo\"],[1729,3.0,null],[null,17.0,null]]}}";

  @Before
  public void setUp() throws Exception {
    SimpleModule mod = new SimpleModule("exhibit", Version.unknownVersion());
    mod.addSerializer(Exhibit.class, new ExhibitSerializer());
    mod.addSerializer(Frame.class, new FrameSerializer());
    mod.addSerializer(ExhibitId.class, new ExhibitIdSerializer());
    mapper = new ObjectMapper();
    mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    mapper.registerModule(mod);
  }

  @Test
  public void testObsAvroJson() throws Exception {
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("a", 1729);
    r1.put("c", "foo");
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("a", 1729);
    r2.put("b", 3.0);
    GenericData.Record r3 = new GenericData.Record(schema);
    r3.put("b", 17.0);
    AvroFrame frame = new AvroFrame(ImmutableList.of(r1, r2, r3));
    AvroFrame emptyFrame = new AvroFrame(new AvroObsDescriptor(schema));
    Exhibit e = SimpleExhibit.of("t1", frame, "e", emptyFrame);
    final String exhibitJson = mapper.writeValueAsString(e);
    assertEquals(expected2, exhibitJson);
  }

  @Test
  public void testObsBsonJson() throws Exception {
    BSONObsDescriptor d = new BSONObsDescriptor(
            ImmutableList.of("a", "b", "c"),
            ImmutableList.of(ObsDescriptor.FieldType.INTEGER, ObsDescriptor.FieldType.DOUBLE,
                    ObsDescriptor.FieldType.STRING));
    BSONFrame frame = new BSONFrame(d, ImmutableList.of(
            new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "c", "foo")),
            new BasicDBObject(ImmutableMap.<String, Object>of("a", 1729, "b", 3.0)),
            new BasicDBObject(ImmutableMap.<String, Object>of("b", 17.0))));
    BSONFrame emptyFrame = new BSONFrame(d, ImmutableList.<BasicDBObject>of());
    Exhibit e = SimpleExhibit.of("t1", frame, "e", emptyFrame);
    final String exhibitJson = mapper.writeValueAsString(e);
    assertEquals(expected2, exhibitJson);
  }
}
