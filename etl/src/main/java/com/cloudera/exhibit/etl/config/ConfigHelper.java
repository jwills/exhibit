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
package com.cloudera.exhibit.etl.config;

import com.cloudera.exhibit.core.PivotCalculator;
import com.esotericsoftware.yamlbeans.YamlReader;

import java.io.FileReader;

public class ConfigHelper {
  public static ComputeConfig parseComputeConfig(String configFile) throws Exception {
    YamlReader reader = new YamlReader(new FileReader(configFile));
    setupComputeReader(reader);
    return reader.read(ComputeConfig.class);
  }

  private static void setupComputeReader(YamlReader reader) throws Exception {
    reader.getConfig().setPropertyElementType(ComputeConfig.class, "tempTables", FrameConfig.class);
    reader.getConfig().setPropertyElementType(ComputeConfig.class, "outputTables", OutputConfig.class);
    reader.getConfig().setPropertyElementType(OutputConfig.class, "aggregates", AggConfig.class);
    reader.getConfig().setPropertyElementType(PivotConfig.class, "variables", PivotCalculator.Key.class);
  }

  public static BuildConfig parseBuildConfig(String configFile) throws Exception {
    YamlReader reader = new YamlReader(new FileReader(configFile));
    reader.getConfig().setPropertyElementType(BuildConfig.class, "sources", SourceConfig.class);
    setupComputeReader(reader);
    return reader.read(BuildConfig.class);
  }
}
