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

public abstract class AbstractTbl implements Tbl {

  private final int id;
  private final String interJson;
  private final String outputJson;
  private transient Schema inter;
  private transient Schema output;

  protected AbstractTbl(int id, Schema inter, Schema output) {
    this.id = id;
    this.interJson = inter.toString();
    this.inter = inter;
    this.outputJson = output.toString();
    this.output = output;
  }

  @Override
  public int getId() {
    return 0;
  }

  @Override
  public Schema intermediateSchema() {
    if (inter == null) {
      inter = parse(interJson);
    }
    return inter;
  }

  @Override
  public Schema finalSchema() {
    if (output == null) {
      output = parse(outputJson);
    }
    return output;
  }

  private static Schema parse(String string) {
    return (new Schema.Parser()).parse(string);
  }
}
