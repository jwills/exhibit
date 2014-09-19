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
package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.BaseEnumerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.BitSet;
import java.util.List;

public class AvroEnumerator extends BaseEnumerator {

  private final List<? extends IndexedRecord> records;
  private final Schema schema;
  private final BitSet stringFields;

  public AvroEnumerator(List<? extends IndexedRecord> records, Schema schema, BitSet stringFields) {
    super(records.size(), schema.getFields().size());
    this.records = records;
    this.schema = schema;
    this.stringFields = stringFields;
  }

  @Override
  protected void updateValues(int index, Object[] current) {
    IndexedRecord record = records.get(index);
    for (int i = 0; i < schema.getFields().size(); i++) {
      if (stringFields.get(i)) {
        Object r = record.get(i);
        current[i] = r == null ? null : r.toString();
      } else {
        current[i] = record.get(i);
      }
    }
  }
}
