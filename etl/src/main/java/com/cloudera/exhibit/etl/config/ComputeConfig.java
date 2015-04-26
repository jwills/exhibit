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
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.Pipeline;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.crunch.CrunchDatasets;

import java.util.List;
import java.util.Map;

public class ComputeConfig {
  public String uri = "";
  public String path = "";
  public int parallelism = -1;
  public boolean local = false;
  public List<ReadableConfig> memoryTables = Lists.newArrayList();
  public List<FrameConfig> tempTables = Lists.newArrayList();
  public List<OutputConfig> outputTables = Lists.newArrayList();

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
