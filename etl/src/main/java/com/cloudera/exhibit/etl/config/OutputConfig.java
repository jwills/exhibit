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
package com.cloudera.exhibit.etl.config;

import com.google.common.collect.Lists;
import org.apache.crunch.Target;

import java.io.Serializable;
import java.util.List;

public class OutputConfig implements Serializable {
  // The Kite URI to write the output to (required)
  public String uri = "";
  // The underlying path that the data should be written to (required)
  public String path = "";

  // The output format to use (parquet or avro)
  public String format = "parquet";

  public Target.WriteMode writeMode = Target.WriteMode.OVERWRITE;

  // A single frame that can be used for non-aggregated output tables
  // that run as map-only jobs
  public FrameConfig collect = null;

  // Any attributes of the Exhibit that should be included in the output keys
  public List<String> attrs = Lists.newArrayList();

  // The names of the fields from the output frames that should be included in the output keys
  public List<String> keys = Lists.newArrayList();

  // The aggregations to perform by the attrs/keys specified above.
  public List<AggConfig> aggregates = Lists.newArrayList();
}
