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

import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class FrameSerializer extends JsonSerializer<Frame> {
  @Override
  public void serialize(Frame res, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeArrayFieldStart("columns");
    for (int i = 0; i < res.descriptor().size(); i++) {
      gen.writeString(res.descriptor().get(i).name);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart("data");
    for (Obs obs : res) {
      gen.writeStartArray();
      for (int i = 0; i < res.descriptor().size(); i++) {
        gen.writeObject(obs.get(i));
      }
      gen.writeEndArray();
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }
}
