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
import org.apache.avro.Schema;
import org.apache.crunch.Target;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import java.util.List;

public class BuildConfig {
  public String uri;

  public String path;

  public String format = "avro";

  public String compress = "uncompressed";

  public String name;

  public List<String> keys;

  public List<String> keyTypes;

  public Target.WriteMode writeMode = Target.WriteMode.OVERWRITE;

  public int parallelism = -1;

  public List<SourceConfig> sources = Lists.newArrayList();

  public ComputeConfig compute = null;
}
