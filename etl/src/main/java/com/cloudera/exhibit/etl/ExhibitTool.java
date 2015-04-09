/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.core.PivotCalculator;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.crunch.CrunchDatasets;

import java.io.FileReader;
import java.util.List;

public class ExhibitTool extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: (build|compute) <config.yml>");
      return -1;
    }
    if ("build".equalsIgnoreCase(args[0])) {
      return build(args[1]);
    } else if ("compute".equalsIgnoreCase(args[0])) {
      return compute(args[1]);
    } else {
      System.err.println("Usage: (build|compute) <config.yml>");
      return -1;
    }
  }

  int compute(String arg) throws Exception {
    ComputeConfig config = parseComputeConfig(arg);
    Pipeline p = new MRPipeline(ExhibitTool.class, getConf());
    Dataset<GenericRecord> data = Datasets.load(config.uri);
    PCollection<GenericRecord> input = p.read(CrunchDatasets.asSource(data));
    for (ComputedConfig computed : config.compute) {
      EvalMetrics evalMetrics = new EvalMetrics(computed.metrics);
      PCollection<GenericData.Record> out = evalMetrics.apply(input);
      DatasetDescriptor dd = new DatasetDescriptor.Builder()
              .schema(((AvroType) out.getPType()).getSchema())
              .format(Formats.PARQUET)
              .build();
      Dataset<GenericRecord> outputDataset = Datasets.create(computed.uri, dd);
      out.write(CrunchDatasets.asTarget(outputDataset), computed.writeMode);
    }
    PipelineResult res = p.done();
    return res.succeeded() ? 0 : 1;
  }

  int build(String arg) throws Exception {
    BuildConfig config = parseBuildConfig(arg);
    Pipeline p = new MRPipeline(ExhibitTool.class, getConf());
    List<PCollection<GenericRecord>> pcols = Lists.newArrayList();
    List<Schema> schemas = Lists.newArrayList();
    for (SourceConfig src : config.sources) {
      Dataset<GenericRecord> data = Datasets.load(src.uri);
      PCollection<GenericRecord> pcol = p.read(CrunchDatasets.asSource(data));
      pcols.add(pcol);
      Schema schema = ((AvroType) pcol.getPType()).getSchema();
      src.setSchema(schema);
      schemas.add(schema);
    }

    // Hack to union the various schemas that will get processed together.
    Schema wrapper = Schema.createRecord("ExhibitWrapper", "crunch", "", false);
    Schema unionSchema = Schema.createUnion(schemas);
    Schema.Field sf = new Schema.Field("value", unionSchema, null, null);
    wrapper.setFields(Lists.newArrayList(sf));
    AvroType<GenericData.Record> valueType = Avros.generics(wrapper);

    AvroType<Pair<Integer, GenericData.Record>> ssType = Avros.pairs(Avros.ints(), valueType);
    PType<Object> keyType = (PType<Object>) config.keyType.getPType();
    PTableType<Object, Pair<Integer, GenericData.Record>> tableType = Avros.tableOf(keyType, ssType);
    PTable<Object, Pair<Integer, GenericData.Record>> union = null;
    for (int i = 0; i < config.sources.size(); i++) {
      SourceConfig src = config.sources.get(i);
      PCollection<GenericRecord> in = pcols.get(i);
      KeyIndexFn<GenericRecord> keyFn = new KeyIndexFn<GenericRecord>(valueType, src.keyFields, src.invalidKeys, i);
      PTable<Object, Pair<Integer, GenericData.Record>> keyed = in.parallelDo("src " + i, keyFn, tableType);
      if (union == null) {
        union = keyed;
      } else {
        union = union.union(keyed);
      }
    }
    MergeSchema ms = new MergeSchema(config.name, config.keyField, config.keyType.getSchema(), config.sources,
        config.parallelism);
    PCollection<GenericData.Record> output = ms.apply(union);
    DatasetDescriptor dd = new DatasetDescriptor.Builder()
        .schema(((AvroType) output.getPType()).getSchema())
        .format(Formats.PARQUET)
        .build();
    Dataset<GenericRecord> outputDataset = Datasets.create(config.uri, dd);
    output.write(CrunchDatasets.asTarget(outputDataset), config.writeMode);
    PipelineResult res = p.done();
    return res.succeeded() ? 0 : 1;
  }

  private ComputeConfig parseComputeConfig(String configFile) throws Exception {
    YamlReader reader = new YamlReader(new FileReader(configFile));
    reader.getConfig().setPropertyElementType(ComputeConfig.class, "compute", ComputedConfig.class);
    reader.getConfig().setPropertyElementType(ComputedConfig.class, "metrics", MetricConfig.class);
    reader.getConfig().setPropertyElementType(MetricConfig.class, "pivot", PivotCalculator.Key.class);
    return reader.read(ComputeConfig.class);
  }

  private BuildConfig parseBuildConfig(String configFile) throws Exception {
    YamlReader reader = new YamlReader(new FileReader(configFile));
    reader.getConfig().setPropertyElementType(BuildConfig.class, "sources", SourceConfig.class);
    reader.getConfig().setPropertyElementType(ComputeConfig.class, "compute", ComputedConfig.class);
    reader.getConfig().setPropertyElementType(ComputedConfig.class, "metrics", MetricConfig.class);
    reader.getConfig().setPropertyElementType(MetricConfig.class, "pivot", PivotCalculator.Key.class);
    return reader.read(BuildConfig.class);
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new ExhibitTool(), args);
    System.exit(rc);
  }
}
