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

import com.cloudera.exhibit.core.ExhibitId;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class ExhibitIdDeserializer extends JsonDeserializer<ExhibitId> {
  @Override
  public ExhibitId deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
    ObjectCodec oc = parser.getCodec();
    JsonNode node = oc.readTree(parser);
    return ExhibitId.create(node.get("entity").asText(), node.get("id").asText());
  }
}
