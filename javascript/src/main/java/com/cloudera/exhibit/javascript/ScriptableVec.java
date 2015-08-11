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
package com.cloudera.exhibit.javascript;

import com.cloudera.exhibit.core.Vec;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class ScriptableVec extends ScriptableObject {

  private final Vec vec;

  public ScriptableVec(Vec vec) {
    this.vec = vec;
  }

  @Override
  public String getClassName() {
    return "Vec";
  }

  public Vec getVec(){
    return this.vec;
  }

  @Override
  public Object get(int index, Scriptable scriptable) {
    return vec.get(index);
  }

  @Override
  public Object get(String property, Scriptable scriptable) {
    if ("length".equals(property)) {
      return vec.size();
    }
    return super.get(property, scriptable);
  }

  @Override
  public boolean has(int index, Scriptable scriptable) {
    return 0 <= index && index < vec.size();
  }

  @Override
  public boolean has(String name, Scriptable scriptable) {
    return "length".equals(name);
  }

  @Override
  public Object getDefaultValue(Class<?> typeHint) {
    return vec.toString();
  }
}
