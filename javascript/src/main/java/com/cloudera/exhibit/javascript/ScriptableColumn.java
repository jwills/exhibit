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

import com.cloudera.exhibit.core.Column;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class ScriptableColumn extends ScriptableObject {

  private final Column column;

  public ScriptableColumn(Column column) {
    this.column = column;
  }

  @Override
  public String getClassName() {
    return "Column";
  }

  @Override
  public Object get(int index, Scriptable scriptable) {
    return column.get(index);
  }

  @Override
  public Object get(String property, Scriptable scriptable) {
    if ("length".equals(property)) {
      return column.size();
    }
    return super.get(property, scriptable);
  }

  @Override
  public boolean has(int index, Scriptable scriptable) {
    return 0 <= index && index < column.size();
  }

  @Override
  public boolean has(String name, Scriptable scriptable) {
    return "length".equals(name);
  }

  @Override
  public Object getDefaultValue(Class<?> typeHint) {
    return column.toString();
  }
}
