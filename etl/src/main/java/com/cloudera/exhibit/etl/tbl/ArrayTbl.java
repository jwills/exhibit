/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.etl.tbl;

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.cloudera.exhibit.etl.SchemaUtil.unwrapNull;

public class ArrayTbl implements Tbl {

  private final boolean distinct;
  private final String name;
  private final String[] inputFields;
  private final String[] outputFields;
  private Schema inner;
  private Schema outer;
  private Collection<GenericData.Record> values;

  public ArrayTbl(boolean distinct, String name, Map<String, String> values) {
    this.distinct = distinct;
    this.name = name;
    this.values = distinct ? Sets.<GenericData.Record>newHashSet() :
        Lists.<GenericData.Record>newArrayList();
    this.inputFields = new String[values.size()];
    this.outputFields = new String[values.size()];
    int index = 0;
    for (Map.Entry<String, String> e : values.entrySet()) {
      inputFields[index] = e.getKey();
      outputFields[index] = e.getValue();
      index++;
    }
  }

  @Override
  public int arity() {
    return 1;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < inputFields.length; i++) {
      ObsDescriptor.Field f = od.get(od.indexOf(inputFields[i]));
      fields.add(new Schema.Field(outputFields[i], AvroExhibit.getSchema(f.type), "", null));
    }
    Schema inner = Schema.createRecord("ExArrayInnerValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    inner.setFields(fields);

    Schema outer = Schema.createRecord("ExArrayValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    List<Schema.Field> outerFields = Lists.newArrayList();
    Schema array = Schema.createArray(inner);

    outerFields.add(new Schema.Field(name,
        Schema.createUnion(Lists.newArrayList(array, Schema.create(Schema.Type.NULL))),
        "", null));
    outer.setFields(outerFields);

    return new SchemaProvider(ImmutableList.of(outer, outer));
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.outer = provider.get(0);
    this.inner = unwrapNull(outer.getField(name).schema()).getElementType();
  }

  @Override
  public void add(Obs obs) {
    GenericData.Record cur = new GenericData.Record(inner);
    for (int i = 0; i < cur.getSchema().getFields().size(); i++) {
      cur.put(i, obs.get(cur.getSchema().getFields().get(i).name()));
    }
    this.values.add(cur);
  }

  @Override
  public GenericData.Record getValue() {
    GenericData.Record ret = new GenericData.Record(outer);
    ret.put(0, Lists.newArrayList(values));
    return ret;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    if (current == null) {
      return next;
    }
    Collection<GenericData.Record> merged = distinct ?
        Sets.<GenericData.Record>newHashSet() : Lists.<GenericData.Record>newArrayList();
    if (current.get(0) != null) {
      merged.addAll((List<GenericData.Record>) current.get(0));
    }
    if (next.get(0) != null) {
      merged.addAll((List<GenericData.Record>) next.get(0));
    }
    current.put(0, Lists.newArrayList(merged));
    return current;
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record value) {
    return ImmutableList.of(value);
  }
}
