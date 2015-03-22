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
package com.cloudera.exhibit.javascript;

import com.cloudera.exhibit.core.Obs;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class ScriptableObs extends ScriptableObject {

  private final Obs obs;

  public ScriptableObs(Obs obs) {
    this.obs = obs;
  }

  @Override
  public String getClassName() {
    return "Obs";
  }

  @Override
  public Object[] getIds() {
    Object[] ids = new Object[obs.descriptor().size()];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = obs.descriptor().get(i).name;
    }
    return ids;
  }

  @Override
  public Object get(String name, Scriptable scriptable) {
    return obs.get(name);
  }

  @Override
  public boolean has(String name, Scriptable scriptable) {
    return obs.descriptor().indexOf(name) > -1;
  }

  @Override
  public Object getDefaultValue(Class<?> typeHint) {
    return obs.toString();
  }
}
