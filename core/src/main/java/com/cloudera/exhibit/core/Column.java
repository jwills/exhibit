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
package com.cloudera.exhibit.core;

import java.util.AbstractList;

public class Column extends AbstractList<Object> {

  private final Frame frame;
  private final int index;

  public static Column create(Frame frame, String name) {
    return create(frame, frame.descriptor().indexOf(name));
  }

  public static Column create(Frame frame, int index) {
    return new Column(frame, index);
  }

  public Column(Frame frame, int index) {
    this.frame = frame;
    this.index = index;
  }

  @Override
  public Object get(int i) {
    return frame.get(i).get(index);
  }

  @Override
  public int size() {
    return frame.size();
  }
}
