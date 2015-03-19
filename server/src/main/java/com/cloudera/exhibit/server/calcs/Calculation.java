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
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.FrameCalculator;
import com.cloudera.exhibit.sql.SQLCalculator;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;

public class Calculation {

  private int id;
  private String code;
  private FrameCalculator calculator;
  private boolean initialized;

  public static SQLCalculator parseSql(String sqlCode) {
    if (sqlCode == null) {
      return null;
    }
    List<String> ret = Lists.newArrayList();
    //TODO: sql comment filtering
    for (String q : Splitter.on(';').trimResults().omitEmptyStrings().split(sqlCode)) {
      ret.add(q);
    }
    return new SQLCalculator(ret.toArray(new String[0]));
  }

  public Calculation(int id, String code) {
    this.id = id;
    this.code = code;
    this.calculator = parseSql(code);
  }

  public int getId() {
    return id;
  }

  public String getCode() {
    return code;
  }

  public Frame apply(Exhibit e) {
    if (!initialized) {
      calculator.initialize(e.descriptor());
      initialized = true;
    }
    return calculator.apply(e);
  }
}
