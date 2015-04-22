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

import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Scriptable;

class SandboxNativeJavaObject extends NativeJavaObject {
  public SandboxNativeJavaObject(Scriptable scope, Object javaObject, Class staticType) {
    super(scope, javaObject, staticType);
  }

  @Override
  public Object get(String name, Scriptable start) {
    if (name.equals("getClass")) {
      return NOT_FOUND;
    }
    return super.get(name, start);
  }
}
