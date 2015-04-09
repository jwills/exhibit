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
package com.cloudera.exhibit.etl.tbl;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import static com.cloudera.exhibit.etl.SchemaUtil.unwrapNull;

public class SumTbl extends AbstractTbl {

  public SumTbl(int id, Schema schema) {
    super(id, schema, schema);
  }

  @Override
  public Object merge(Object current, Object next) {
    return add(current, next, intermediateSchema());
  }

  @Override
  public Object finalize(Object current) {
    // Nothing to do for sums
    return current;
  }

  public static Object add(Object cur, Object next, Schema schema) {
    if (cur == null) {
      return next;
    } else if (next != null) {
      schema = unwrapNull(schema);
      switch (schema.getType()) {
        case INT:
          return ((Integer) cur) + ((Integer) next);
        case DOUBLE:
          return ((Double) cur) + ((Double) next);
        case FLOAT:
          return ((Float) cur) + ((Float) next);
        case LONG:
          return ((Long) cur) + ((Long) next);
        case RECORD:
          IndexedRecord rc = (IndexedRecord) cur;
          IndexedRecord nc = (IndexedRecord) next;
          for (int i = 0; i < schema.getFields().size(); i++) {
            rc.put(i, add(rc.get(i), nc.get(i), schema.getFields().get(i).schema()));
          }
          return rc;
        default:
          throw new UnsupportedOperationException("Cannot sum non-numeric type: " + schema.getType());
      }
    } else {
      return cur;
    }
  }

}
