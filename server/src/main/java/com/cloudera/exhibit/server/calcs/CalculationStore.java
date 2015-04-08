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
package com.cloudera.exhibit.server.calcs;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Obs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class CalculationStore {

  private List<Calculation> calculations;

  public CalculationStore() {
    // TODO: remove this
    this.calculations = Lists.newArrayList();
    addCalculation("select (sum(yds)/count(distinct gid)) pass_ypg from passes");
    addCalculation("select (sum(yds)/count(distinct gid)) rush_ypg from rushes");
  }

  public synchronized Map<String, Map<String, Object>> computeKPIs(Exhibit exhibit) {
    Map<String, Map<String, Object>> ret = Maps.newHashMap();
    if (exhibit == null) {
      return ret;
    }

    for (Calculation calc : calculations) {
      Iterable<Obs> frame = calc.apply(exhibit);
      for (Obs obs : frame) {
        // TODO: multi row? Real objects, probably?
        for (int i = 0; i < obs.descriptor().size(); i++) {
          Map<String, Object> base = Maps.newHashMap();
          base.put("id", calc.getId());
          base.put("value", obs.get(i));
          ret.put(obs.descriptor().get(i).name, base);
        }
      }
    }
    return ret;
  }

  public synchronized void addCalculation(String code) {
    int id = calculations.size();
    Calculation c = new Calculation(id, code);
    calculations.add(c);
  }

  public synchronized String getCode(int id) {
    return calculations.get(id).getCode();
  }
}
