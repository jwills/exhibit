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
package com.cloudera.exhibit.etl;

import com.google.common.collect.Lists;
import org.apache.crunch.Target;

import java.util.List;

public class ComputedConfig {
  public String uri = "";
  public Target.WriteMode writeMode = Target.WriteMode.OVERWRITE;

  public List<MetricConfig> metrics = Lists.newArrayList();

  // Aggregation-related configuration fields that are computed by
  // the metrics associated with this compute configuration. Note
  // that ALL of the metrics must define ALL of the keys for this
  // to work properly.
  public List<String> keys = Lists.newArrayList();

  // The name of the count field, if any
  public String count;

  // Perform sums on the following columns.
  public List<String> sum = Lists.newArrayList();

  // Count the distinct number of elements for the following columns
  public List<String> distinct = Lists.newArrayList();

  // Track the minimum element for the following columns
  public List<String> min = Lists.newArrayList();

  // Track the maximum element for the following columns.
  public List<String> max = Lists.newArrayList();
}
