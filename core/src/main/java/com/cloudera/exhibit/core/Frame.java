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
package com.cloudera.exhibit.core;

public abstract class Frame implements Iterable<Obs> {
  public abstract ObsDescriptor descriptor();
  public abstract int size();
  public abstract Obs get(int rowIndex);

  public Column $(int columnIndex) {
    return Column.create(this, columnIndex);
  }

  public Column $(String columnName) {
    return Column.create(this, columnName);
  }
}
