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
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * <p>Configuration information used to pivot a given frame computation. It's best to explain
 * how this works with an example.</p>
 *
 * <p>Let's say we have a frame computation against a table called "users" that looks like this:
 *
 * <br/><code>SELECT age, sex, sum(income) inc, count(*) cnt FROM users GROUP BY age, sex;</code>
 *
 * <br/>For each age, we will get two rows corresponding to the sum of incomes and counts for
 * males and females. If we want our output frame to have a single row for each age that
 * contains four columns (inc_male, inc_female, cnt_male, cnt_female), then we can use a
 * pivot operation on the resulting table, like this sample YAML snippet:
 *
 * <br/><code>pivot: {by: [age], variables: {sex: [male, female]}}</code></p>
 *
 */
public class PivotConfig implements Serializable {
  public List<String> by = Lists.newArrayList();

  // A mapping of variable names to a list of valid values that they can take on
  // that will be used to generate the pivot columns.
  public Map<String, List<String>> variables = Maps.newLinkedHashMap();
}
