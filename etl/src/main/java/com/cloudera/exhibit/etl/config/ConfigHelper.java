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

import com.esotericsoftware.yamlbeans.YamlReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Source;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;

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
    reader.getConfig().setPropertyElementType(ComputeConfig.class, "memoryTables", ReadableConfig.class);
    reader.getConfig().setPropertyElementType(OutputConfig.class, "aggregates", AggConfig.class);
  }

  public static PCollection<GenericData.Record> getPCollection(Pipeline p, String uri, String pathStr) {
    if (pathStr != null && !pathStr.isEmpty()) {
      // NOTE: this is for backwards compatibility, think about removing this
      return p.read(From.avroFile(pathStr));
    }
    Dataset ds = Datasets.load(uri);
    Path path = new Path(ds.getDescriptor().getLocation());
    Format fmt = ds.getDescriptor().getFormat();
    Schema schema = ds.getDescriptor().getSchema();
    AvroType<GenericData.Record> ptype = Avros.generics(schema);
    Source<GenericData.Record> src;
    if (Formats.AVRO.equals(fmt)) {
      src = From.avroFile(path, ptype);
    } else if (Formats.PARQUET.equals(fmt)) {
      src = new AvroParquetFileSource<GenericData.Record>(path, ptype);
    } else {
      throw new IllegalArgumentException("Cannot handle input format: " + fmt + " of uri: " + uri);
    }
    return p.read(src);
  }

  public static BuildConfig parseBuildConfig(String configFile) throws Exception {
    YamlReader reader = new YamlReader(new FileReader(configFile));
    reader.getConfig().setPropertyElementType(BuildConfig.class, "sources", SourceConfig.class);
    setupComputeReader(reader);
    return reader.read(BuildConfig.class);
  }
}
