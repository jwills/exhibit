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

import com.cloudera.exhibit.core.Frame;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class ScriptableFrame extends ScriptableObject {

  private final Frame frame;

  public ScriptableFrame(Frame frame) {
    super();
    this.frame = frame;
  }

  public Frame frame() { return frame; }

  @Override
  public String getClassName() {
    return "Frame";
  }

  @Override
  public Object[] getIds() {
    Object[] ids = new Object[frame.descriptor().size()];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = frame.descriptor().get(i).name;
    }
    return ids;
  }

  @Override
  public Object get(int index, Scriptable scriptable) {
    return new ScriptableObs(frame.get(index));
  }

  @Override
  public Object get(String name, Scriptable scriptable) {
    if ("length".equals(name)) {
      return frame.size();
    }
    return new ScriptableVec(frame.$(name));
  }

  @Override
  public boolean has(String name, Scriptable scriptable) {
    return frame.descriptor().indexOf(name) > -1;
  }

  @Override
  public Object getDefaultValue(Class<?> typeHint) {
    return frame.toString();
  }
}
