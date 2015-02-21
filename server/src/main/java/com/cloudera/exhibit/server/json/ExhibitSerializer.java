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
package com.cloudera.exhibit.server.json;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Map;

public class ExhibitSerializer extends JsonSerializer<Exhibit> {

  @Override
  public void serialize(Exhibit exhibit, JsonGenerator gen, SerializerProvider provider) throws IOException {
    ExhibitDescriptor desc = exhibit.descriptor();
    gen.writeStartObject();
    gen.writeObjectFieldStart("attrs");
    serializeObs(exhibit.attributes(), exhibit.attributes().descriptor(), gen);
    gen.writeEndObject();
    for (Map.Entry<String, ObsDescriptor> fd : desc.frames().entrySet()) {
      gen.writeArrayFieldStart(fd.getKey());
      for (Obs obs : exhibit.frames().get(fd.getKey())) {
        gen.writeStartObject();
        serializeObs(obs, fd.getValue(), gen);
        gen.writeEndObject();
      }
      gen.writeEndArray();
    }
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
            gen.writeNumberField(f.name, (Integer) value);
            break;
          case FLOAT:
            gen.writeNumberField(f.name, (Float) value);
            break;
          case DOUBLE:
            gen.writeNumberField(f.name, (Double) value);
            break;
          case LONG:
            gen.writeNumberField(f.name, (Long) value);
            break;
          default:
            throw new UnsupportedOperationException("Unknown type for field: " + f);
        }
      }
    }
  }
}

