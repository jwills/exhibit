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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Set;

public class ExhibitSerializer extends JsonSerializer<Exhibit> {

  @Override
  public void serialize(Exhibit exhibit, JsonGenerator gen, SerializerProvider provider) throws IOException {
    ExhibitDescriptor desc = exhibit.descriptor();
    // start object
    gen.writeStartObject();

    // Write attrs
    gen.writeObjectFieldStart("attrs");
    serializeObs(exhibit.attributes(), exhibit.attributes().descriptor(), gen);
    gen.writeEndObject();

    // Write frame column names
    gen.writeObjectFieldStart("columns");
    Set<String> frameNames = Sets.newTreeSet(desc.frames().keySet());
    for (String frameName : frameNames) {
      gen.writeArrayFieldStart(frameName);
      for (ObsDescriptor.Field f : desc.frames().get(frameName)) {
        // TODO: type info?
        gen.writeString(f.name);
      }
      gen.writeEndArray();
    }
    gen.writeEndObject();

    // Write frame obs
    gen.writeObjectFieldStart("frames");
    for (String frameName : frameNames) {
      gen.writeArrayFieldStart(frameName);
      for (Obs obs : exhibit.frames().get(frameName)) {
        gen.writeStartArray();
        serializeObsArray(obs, desc.frames().get(frameName), gen);
        gen.writeEndArray();
      }
      gen.writeEndArray();
    }
    gen.writeEndObject();

    // finish object
    gen.writeEndObject();
  }

  private void serializeObs(Obs obs, ObsDescriptor desc, JsonGenerator gen) throws IOException {
    for (int i = 0; i < desc.size(); i++) {
      ObsDescriptor.Field f = desc.get(i);
      Object value = obs.get(i);
      if (value == null) {
        gen.writeNullField(f.name);
      } else {
        switch (f.type) {
          case STRING:
            gen.writeStringField(f.name, value.toString());
            break;
          case BOOLEAN:
            gen.writeBooleanField(f.name, (Boolean) value);
            break;
          case INTEGER:
            gen.writeNumberField(f.name, ((Number) value).intValue());
            break;
          case FLOAT:
            gen.writeNumberField(f.name, ((Number) value).floatValue());
            break;
          case DOUBLE:
            gen.writeNumberField(f.name, ((Number) value).doubleValue());
            break;
          case LONG:
            gen.writeNumberField(f.name, ((Number) value).longValue());
            break;
          default:
            throw new UnsupportedOperationException("Unknown type for field: " + f);
        }
      }
    }
  }

  private void serializeObsArray(Obs obs, ObsDescriptor desc, JsonGenerator gen) throws IOException {
    for (int i = 0; i < desc.size(); i++) {
      ObsDescriptor.Field f = desc.get(i);
      Object value = obs.get(i);
      if (value == null) {
        gen.writeNull();
      } else {
        switch (f.type) {
          case STRING:
            gen.writeString(value.toString());
            break;
          case BOOLEAN:
            gen.writeBoolean((Boolean) value);
            break;
          case INTEGER:
            gen.writeNumber(((Number) value).intValue());
            break;
          case FLOAT:
            gen.writeNumber(((Number) value).floatValue());
            break;
          case DOUBLE:
            gen.writeNumber(((Number) value).doubleValue());
            break;
          case LONG:
            gen.writeNumber(((Number) value).longValue());
            break;
          default:
            throw new UnsupportedOperationException("Unknown type for field: " + f);
        }
      }
    }
  }
}

