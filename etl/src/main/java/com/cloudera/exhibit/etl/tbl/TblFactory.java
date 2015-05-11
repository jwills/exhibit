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

import com.cloudera.exhibit.etl.SchemaProvider;
import com.cloudera.exhibit.etl.config.BuildOutConfig;
import com.cloudera.exhibit.etl.config.OutputConfig;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

public abstract class TblFactory implements Serializable {

  private final List<List<SchemaProvider>> providersList;

  public TblFactory(List<List<SchemaProvider>> providersList) {
    this.providersList = providersList;
  }

  public List<List<Tbl>> createAll() {
    List<List<Tbl>> all = Lists.newArrayList();
    for (int i = 0; i < providersList.size(); i++) {
      List<Tbl> cur = Lists.newArrayList();
      for (int j = 0; j < providersList.get(i).size(); j++) {
        cur.add(create(i, j));
      }
      all.add(cur);
    }
    return all;
  }

  public Tbl create(int outputId, int componentId) {
    Tbl tbl = createInner(outputId, componentId);
    if (tbl == null) {
      return null;
    }
    tbl.initialize(providersList.get(outputId).get(componentId));
    return tbl;
  }

  protected abstract Tbl createInner(int ouputId, int componentId);

  public static class Compute extends TblFactory {

    private final List<OutputConfig> outputs;

    public Compute(List<OutputConfig> outputs, List<List<SchemaProvider>> providersList) {
      super(providersList);
      this.outputs = outputs;
    }

    @Override
    protected Tbl createInner(int outputId, int componentId) {
      return outputs.get(outputId).aggregates.get(componentId).createTbl();
    }
  }

  public static class Build extends TblFactory {

    private final List<BuildOutConfig> outputs;

    public Build(List<BuildOutConfig> outputs, List<List<SchemaProvider>> providersList) {
      super(providersList);
      this.outputs = outputs;
    }

    @Override
    protected Tbl createInner(int outputId, int componentId) {
      return outputs.get(outputId).components.get(componentId).createTbl();
    }
  }
}
