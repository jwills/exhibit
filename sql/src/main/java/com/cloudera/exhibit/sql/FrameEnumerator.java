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
package com.cloudera.exhibit.sql;

import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import org.apache.calcite.linq4j.Enumerator;

public class FrameEnumerator implements Enumerator<Object> {

  private final Frame frame;
  private Object[] current;
  private int currentIndex = -1;

  public FrameEnumerator(Frame frame) {
    this.frame = frame;
    this.current = new Object[frame.descriptor().size()];
  }

  @Override
  public Object current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    currentIndex++;
    boolean hasNext = currentIndex < frame.size();
    if (hasNext) {
      Obs obs = frame.get(currentIndex);
      for (int i = 0; i < current.length; i++) {
        current[i] = obs.get(i);
      }
    }
    return hasNext;

  }

  @Override
  public void reset() {
    currentIndex = -1;
    current = new Object[current.length];
  }

  @Override
  public void close() {
    // No-op
  }
}
