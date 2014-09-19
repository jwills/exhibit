/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.core;

import net.hydromatic.linq4j.Enumerator;

public abstract class BaseEnumerator implements Enumerator<Object> {

  private final int numRecords;
  private Object[] current;
  private int currentIndex = -1;

  public BaseEnumerator(int numRecords, int numFields) {
    this.numRecords = numRecords;
    this.current = new Object[numFields];
  }

  @Override
  public Object current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    currentIndex++;
    boolean hasNext = currentIndex < numRecords;
    if (hasNext) {
      updateValues(currentIndex, current);
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

  abstract protected void updateValues(int index, Object[] current);
}
