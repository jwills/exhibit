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
package com.cloudera.exhibit.etl.tbl;

import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.SchemaProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.List;
import java.util.Map;

public class RatioTbl implements Tbl {

  private static final String RATIO = "ratio";
  private static final String NUM = "numerator";
  private static final String DEN = "denominator";

  private String outputField;
  private String numeratorField;
  private String denominatorField;
  private Schema intermediate;
  private Schema output;

  private double currentRatio;
  private double currentDenominator;

  public RatioTbl(Map<String, String> values, Map<String, Object> options) {
    if (values.size() > 1) {
      throw new IllegalArgumentException("RATIO table only has one output element");
    }
    this.outputField = Iterables.getOnlyElement(values.entrySet()).getValue();

    if (!options.containsKey(NUM)) {
      throw new IllegalArgumentException("RATIO table must have 'numerator' field in options list");
    }
    if (!options.containsKey(DEN)) {
      throw new IllegalArgumentException("RATIO table must have 'denominator' field in options list");
    }

    this.numeratorField = options.get(NUM).toString();
    this.denominatorField = options.get(DEN).toString();
  }

  @Override
  public int arity() {
    return 1;
  }

  @Override
  public SchemaProvider getSchemas(ObsDescriptor od, int outputId, int aggIdx) {
    boolean hasNum = false, hasDen = false;
    for (ObsDescriptor.Field f : od) {
      if (numeratorField.equals(f.name)) {
        hasNum = true;
      } else if (denominatorField.equals(f.name)) {
        hasDen = true;
      }
    }
    if (!hasNum) {
      throw new IllegalStateException("Could not find numerator field '" + numeratorField + "' in descriptor");
    }
    if (!hasDen) {
      throw new IllegalStateException("Could not find denominator field '" + denominatorField + "' in descriptor");
    }
    List<Schema.Field> interFields = Lists.newArrayList();
    interFields.add(new Schema.Field(RATIO, Schema.create(Schema.Type.DOUBLE), "", null));
    interFields.add(new Schema.Field(DEN, Schema.create(Schema.Type.DOUBLE), "", null));
    this.intermediate = Schema.createRecord("InterValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    this.intermediate.setFields(interFields);

    List<Schema.Field> outputFields = Lists.newArrayList();
    outputFields.add(new Schema.Field(outputField, Schema.create(Schema.Type.DOUBLE), "", null));
    this.output = Schema.createRecord("OutputValue_" + outputId + "_" + aggIdx, "", "exhibit", false);
    this.output.setFields(outputFields);

    return new SchemaProvider(ImmutableList.of(intermediate, output));
  }

  @Override
  public void initialize(SchemaProvider provider) {
    this.intermediate = provider.get(0);
    this.output = provider.get(1);
  }

  @Override
  public void add(Obs obs) {
    double nextNum = ((Number) obs.get(numeratorField)).doubleValue();
    double nextDen = ((Number) obs.get(denominatorField)).doubleValue();
    currentRatio = (currentRatio * currentDenominator + nextNum) / (currentDenominator + nextDen);
    currentDenominator += nextDen;
  }

  @Override
  public GenericData.Record getValue() {
    GenericData.Record r = new GenericData.Record(intermediate);
    r.put(RATIO, currentRatio);
    r.put(DEN, currentDenominator);
    return r;
  }

  @Override
  public GenericData.Record merge(GenericData.Record current, GenericData.Record next) {
    currentRatio = (Double) current.get("ratio");
    currentDenominator = (Double) current.get(DEN);

    double nextDen = (Double) next.get(DEN);
    double nextNum = nextDen * ((Double) next.get(RATIO));

    currentRatio = (currentRatio * currentDenominator + nextNum) / (currentDenominator + nextDen);
    currentDenominator += nextDen;

    current.put(RATIO, currentRatio);
    current.put(DEN, currentDenominator);
    return current;
  }

  @Override
  public List<GenericData.Record> finalize(GenericData.Record value) {
    GenericData.Record res = new GenericData.Record(output);
    res.put(outputField, value.get(RATIO));
    return ImmutableList.of(res);
  }

  @Override
  public String toString() {
    return "RatioTbl(" + outputField + " = " + numeratorField + "/" + denominatorField + ")";
  }
}
