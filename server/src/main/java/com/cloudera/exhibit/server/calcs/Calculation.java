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

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.sql.SQLCalculator;

public class Calculation {

  private int id;
  private String code;
  private Calculator calculator;
  private boolean initialized;


  public Calculation(int id, String code) {
    this.id = id;
    this.code = code;
    this.calculator = SQLCalculator.create(null, code); //TODO
  }

  public int getId() {
    return id;
  }

  public String getCode() {
    return code;
  }

  public Iterable<Obs> apply(Exhibit e) {
    if (!initialized) {
      calculator.initialize(e.descriptor());
      initialized = true;
    }
    return calculator.apply(e);
  }
}
