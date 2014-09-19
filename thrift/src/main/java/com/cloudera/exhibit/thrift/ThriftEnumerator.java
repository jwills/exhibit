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
package com.cloudera.exhibit.thrift;

import com.cloudera.exhibit.core.BaseEnumerator;
import org.apache.thrift.TBase;

import java.util.List;

class ThriftEnumerator extends BaseEnumerator {

  private final List<? extends TBase> records;
  private final ThriftHelper helper;

  public ThriftEnumerator(List<? extends TBase> records, ThriftHelper helper) {
    super(records.size(), helper.getNumFields());
    this.records = records;
    this.helper = helper;
  }
  @Override
  protected void updateValues(int index, Object[] current) {
    TBase tBase = records.get(index);
    for (int i = 0; i < helper.getNumFields(); i++) {
      current[i] = helper.getFieldValue(i, tBase);
    }
  }
}
