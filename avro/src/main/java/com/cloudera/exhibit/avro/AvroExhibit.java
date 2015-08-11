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
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibitDescriptor;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AvroExhibit {

  public static ExhibitDescriptor createDescriptor(Schema schema) {
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    Map<String, ObsDescriptor> frames = Maps.newHashMap();
    Map<String, FieldType> vectors = Maps.newHashMap();
    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field f = schema.getFields().get(i);
      Schema unwrapped = AvroObsDescriptor.unwrap(f.schema());
      if (unwrapped.getType() == Schema.Type.ARRAY) {
        Schema elementType = unwrapped.getElementType();
        if(isRecordElement(elementType)) {
          frames.put(f.name(), new AvroObsDescriptor(getRecordElement(elementType)));
        } else {
          vectors.put(f.name(), AvroObsDescriptor.getFieldType(elementType));
        }
      } else {
        FieldType ft = AvroObsDescriptor.getFieldType(unwrapped);
        if (ft != null) {
          fields.add(new ObsDescriptor.Field(f.name(), ft));
        }
      }
    }
    return new SimpleExhibitDescriptor(new SimpleObsDescriptor(fields), frames, vectors);
  }

  static boolean isRecordElement(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD) {
      return true;
    } else if (schema.getType() == Schema.Type.UNION) {
      Schema unwrappedField = AvroObsDescriptor.unwrap(schema);
      if ( unwrappedField.getType() == Schema.Type.RECORD ){
        return true;
      }
    }
    return false;
  }

  static Schema getRecordElement(Schema schema) {
    // It's records all the way down (!)
    if (schema.getType() == Schema.Type.UNION) {
      Schema unwrappedField = AvroObsDescriptor.unwrap(schema);
      if ( unwrappedField.getType() == Schema.Type.RECORD ){
        return getRecordElement(unwrappedField);
      }
    } else if (schema.getType() == Schema.Type.RECORD) {
      if (schema.getFields().size() == 1) {
        Schema unwrappedField = AvroObsDescriptor.unwrap(schema.getFields().get(0).schema());
        return getRecordElement(unwrappedField);
      } else {
        return schema;
      }
    }
    throw new UnsupportedOperationException("Not a record: " + schema);
  }

  static GenericRecord getInnerRecord(GenericRecord record) {
    if (record.getSchema().getType() == Schema.Type.RECORD) {
      if (record.getSchema().getFields().size() == 1) {
        return getInnerRecord((GenericRecord) record.get(0));
      }
    }
    return record;
  }

  public static Exhibit create(GenericRecord record) {
    Schema schema = record.getSchema();
    ExhibitDescriptor desc = createDescriptor(schema);
    List<Object> attrValues = Lists.newArrayListWithExpectedSize(desc.attributes().size());
    for (int i = 0; i < desc.attributes().size(); i++) {
      Object val = record.get(desc.attributes().get(i).name);
      if (val != null && desc.attributes().get(i).type == FieldType.STRING) {
        val = val.toString();
      }
      attrValues.add(val);
    }
    Map<String, Frame> frames = Maps.newHashMap();
    for (String frameName : desc.frames().keySet()) {
      List<GenericRecord> recs = Lists.newArrayList();
      List<GenericRecord> raw = (List<GenericRecord>) record.get(frameName);
      if (raw != null) {
        for (GenericRecord rawRec : raw) {
          recs.add(getInnerRecord(rawRec));
        }
      }
      frames.put(frameName, new AvroFrame((AvroObsDescriptor) desc.frames().get(frameName), recs));
    }
    Map<String, Vector> vectors = Maps.newHashMap();
    for (Map.Entry<String, FieldType> v: desc.vectors().entrySet()) {
      List vals = Arrays.asList((Object[])record.get(v.getKey()));
      vectors.put(v.getKey(), VectorBuilder.build(v.getValue(), vals));
    }
    return new SimpleExhibit(new SimpleObs(desc.attributes(), attrValues), frames, vectors);
  }

  private AvroExhibit() {}

  public static Schema.Field getSchemaField(ObsDescriptor.Field f) {
    return new Schema.Field(f.name, getSchema(f.type), "", null);
  }

  public static Schema getSchema(FieldType type) {
    Schema internal;
    switch (type) {
      case BOOLEAN:
        internal = Schema.create(Schema.Type.BOOLEAN);
        break;
      case DOUBLE:
        internal = Schema.create(Schema.Type.DOUBLE);
        break;
      case FLOAT:
        internal = Schema.create(Schema.Type.FLOAT);
        break;
      case INTEGER:
        internal = Schema.create(Schema.Type.INT);
        break;
      case LONG:
        internal = Schema.create(Schema.Type.LONG);
        break;
      case SHORT:
        internal = Schema.create(Schema.Type.INT);
        break;
      case STRING:
        internal = Schema.create(Schema.Type.STRING);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported FieldType: " + type);
    }
    return Schema.createUnion(Lists.newArrayList(internal, Schema.create(Schema.Type.NULL)));
  }
}
