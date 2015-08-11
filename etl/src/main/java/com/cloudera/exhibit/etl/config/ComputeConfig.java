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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.Pipeline;
import org.apache.crunch.ReadableData;
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

import java.util.List;
import java.util.Map;

/**
 * Configuration operations for running a series of computations and associated aggregations over
 * a supernova schema in order to generate one or more output tables. Each compute job corresponds
 * to exactly one MapReduce/Spark aggregation, no matter how many output tables it generates.
 */
public class ComputeConfig {
  // The Kite URI of the input supernova record (required).
  public String uri = "";

  // An optional path argument to an Avro file that can be used to bypass Kite URIs (hopefully deprecated soon.)
  public String path = "";

  // The number of reducers to use for performing aggregations. The rule of thumb is ~ 1 reducer per 1GB of
  // output data.
  public int parallelism = -1;

  // An optional local mode to use for testing/debugging.
  public boolean local = false;

  // A list of Kite URIs/paths that contain Hive tables that should be loaded into memory and made available
  // to all subsequent computations (e.g., small dimension tables.)
  public List<ReadableConfig> memoryTables = Lists.newArrayList();

  // A list of frame computations that should be generated for each exhibit and made available to all
  // subsequent computations. Can make use of all memory tables or any temp tables that come earlier
  // in the list.
  public List<FrameConfig> tempTables = Lists.newArrayList();

  // A list of output tables to generate as the result of this compute job. These output computations
  // can make use of any memory or temp tables that have been generated, but they *cannot* see the
  // computations of other output tables.
  public List<OutputConfig> outputTables = Lists.newArrayList();

  public long sleepTimeMsec = 30000L;

  public Map<String, ReadableData<GenericData.Record>> getReadables(Pipeline p) {
    Map<String, ReadableData<GenericData.Record>> ret = Maps.newHashMap();
    for (ReadableConfig rc : memoryTables) {
      Dataset ds = Datasets.load(rc.uri);
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
        throw new IllegalArgumentException("Cannot handle format: " + fmt);
      }
      ret.put(rc.name, p.read(src).asReadable(false));
    }
    return ret;
  }
}
