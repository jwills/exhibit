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
package com.cloudera.exhibit.server.calcs;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Functor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.sql.SQLFunctor;

public class Function {

  private int id;
  private String code;
  private Functor functor;
  private boolean initialized;


  public Function(int id, String code) {
    this.id = id;
    this.code = code;
    this.functor = SQLFunctor.create(null, code); //TODO
  }

  public int getId() {
    return id;
  }

  public String getCode() {
    return code;
  }

  public Iterable<Obs> apply(Exhibit e) {
    if (!initialized) {
      functor.initialize(e.descriptor());
      initialized = true;
    }
    Exhibit result = functor.apply(e);
    Frame resultFrame = result.frames().get(SQLFunctor.DEFAULT_RESULT_FRAME);
    return resultFrame; //TODO: expose exhibit
  }
}
