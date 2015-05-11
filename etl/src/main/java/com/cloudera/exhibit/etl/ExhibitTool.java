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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.avro.AvroExhibit;
import com.cloudera.exhibit.avro.AvroObsDescriptor;
import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.config.AbstractOutputConfig;
import com.cloudera.exhibit.etl.config.Build2Config;
import com.cloudera.exhibit.etl.config.BuildConfig;
import com.cloudera.exhibit.etl.config.BuildOutConfig;
import com.cloudera.exhibit.etl.config.ComponentConfig;
import com.cloudera.exhibit.etl.config.ComputeConfig;
import com.cloudera.exhibit.etl.config.ConfigHelper;
import com.cloudera.exhibit.etl.config.OutputConfig;
import com.cloudera.exhibit.etl.config.Source2Config;
import com.cloudera.exhibit.etl.config.SourceConfig;
import com.cloudera.exhibit.etl.fn.CollectFn;
import com.cloudera.exhibit.etl.fn.ComponentMapFn;
import com.cloudera.exhibit.etl.fn.ExCombiner;
import com.cloudera.exhibit.etl.fn.FilterOutFn;
import com.cloudera.exhibit.etl.fn.KeyIndexFn;
import com.cloudera.exhibit.etl.fn.MergeRowsFn;
import com.cloudera.exhibit.etl.fn.SchemaMapFn;
import com.cloudera.exhibit.etl.tbl.Tbl;
import com.cloudera.exhibit.etl.tbl.TblFactory;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.io.parquet.AvroParquetFileTarget;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.cloudera.exhibit.etl.SchemaUtil.unionKeySchema;
import static com.cloudera.exhibit.etl.SchemaUtil.unionValueSchema;

public class ExhibitTool extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: (build|build2|compute|parse) <config.yml>");
      return -1;
    }
    if ("build".equalsIgnoreCase(args[0])) {
      return build(args[1]);
    } else if ("build2".equalsIgnoreCase(args[0])) {
      return build2(args[1]);
    } else if ("compute".equalsIgnoreCase(args[0])) {
      return compute(args[1]);
    } else if ("parse".equalsIgnoreCase(args[0])) {
      return parse(args[1]);
    } else {
      System.err.println("Usage: (build|compute|parse) <config.yml>");
      return -1;
    }
  }

  int parse(String arg) throws Exception {
    ComputeConfig config = ConfigHelper.parseComputeConfig(arg);
    System.out.println("Config parsed successfully");
    return 0;
  }

  int compute(String ymlFile) throws Exception {
    ComputeConfig config = ConfigHelper.parseComputeConfig(ymlFile);
    Pipeline p = new MRPipeline(ExhibitTool.class, "ComputeSupernova", getConf());
    PCollection<GenericData.Record> input = ConfigHelper.getPCollection(p, config.uri, config.path);
    // Step one: generate additional tempTables, if any.
    RecordToExhibit rte = new RecordToExhibit(config.getReadables(p), config.tempTables);
    ExhibitDescriptor descriptor = rte.getDescriptor(input.getPType());

    PCollection<Exhibit> exhibits = rte.apply(input);

    // Step two: determine the key and value schemas from the outputTables.
    List<OutputGen> outputAggs = Lists.newArrayList();
    Set<Schema> keySchemas = Sets.newHashSet();
    List<List<SchemaProvider>> providerLists = Lists.newArrayList();
    Set<Schema> interValueSchemas = Sets.newHashSet();
    List<Schema> outputSchemas = Lists.newArrayList();
    for (int i = 0; i < config.outputTables.size(); i++) {
      OutputConfig output = config.outputTables.get(i);
      if (output.collect != null) {
        // map-side output
        Calculator c = output.collect.getCalculator();
        ObsDescriptor od = c.initialize(descriptor);
        List<Schema.Field> mapsideFields = Lists.newArrayList();
        for (ObsDescriptor.Field f : od) {
          mapsideFields.add(new Schema.Field(f.name, AvroExhibit.getSchema(f.type), "", null));
        }
        Schema mapsideSchema = Schema.createRecord("ExOutput" + i, "", "exhibit", false);
        mapsideSchema.setFields(mapsideFields);
        PCollection<GenericData.Record> mapOut = exhibits.parallelDo(new CollectFn(output.collect, mapsideSchema),
            Avros.generics(mapsideSchema));
      } else {
        OutputGen gen = new OutputGen(i, output, descriptor);
        Schema keySchema = gen.getKeySchema();
        List<SchemaProvider> providers = gen.getSchemaProviders();

        List<Schema.Field> outputFields = Lists.newArrayList();
        for (Schema.Field sf : keySchema.getFields()) {
          outputFields.add(new Schema.Field(sf.name(), sf.schema(), sf.doc(), sf.defaultValue()));
        }
        for (SchemaProvider sp : providers) {
          Schema s = sp.get(1); // output
          for (Schema.Field sf : s.getFields()) {
            outputFields.add(new Schema.Field(sf.name(), sf.schema(), sf.doc(), sf.defaultValue()));
          }
        }
        Schema outputSchema = Schema.createRecord("ExOutput" + i, "", "exhibit", false);
        outputSchema.setFields(outputFields);
        System.out.println("Output Schema " + i + ": " + outputSchema.toString(true));

        keySchemas.add(keySchema);
        interValueSchemas.addAll(Lists.transform(providers, new Function<SchemaProvider, Schema>() {
          public Schema apply(SchemaProvider schemaProvider) {
            return schemaProvider.get(0); // intermediate
          }
        }));
        outputAggs.add(gen);
        outputSchemas.add(outputSchema);
        providerLists.add(providers);
      }
    }

    if (outputAggs.size() > 0) {
      // Aggregations that require a reduce operation
      Schema keySchema = unionKeySchema("ExhibitKey", Lists.newArrayList(keySchemas));
      Schema interValueSchema = unionValueSchema("ExhibitInterValue", Lists.newArrayList(interValueSchemas));
      SchemaProvider provider = new SchemaProvider(ImmutableList.of(keySchema, interValueSchema));
      AvroType<GenericData.Record> keyType = Avros.generics(keySchema);
      AvroType<GenericData.Record> interValueType = Avros.generics(interValueSchema);
      PTableType<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> ptt = Avros.tableOf(
              Avros.pairs(keyType, Avros.ints()),
              Avros.pairs(Avros.ints(), interValueType));
      // <<Grouping Key, OutputId>, <AggId, AggValue>>
      PTable<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> mapside = null;
      for (int i = 0; i < outputAggs.size(); i++) {
        // First table: <grouping key, <AggIdx, AggValue>>
        PTable<GenericData.Record, Pair<Integer, GenericData.Record>> out = outputAggs.get(i).apply(exhibits);

        // Second table: <<Union of grouping keys, OutputId>, <AggIdx, Aggvalue>>
        PTable<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> m = out.parallelDo(
                new SchemaMapFn(i, provider), ptt);
        mapside = (mapside == null) ? m : mapside.union(m);
      }

      GroupingOptions opts = GroupingOptions.builder()
              .numReducers(config.parallelism)
              .partitionerClass(JoinUtils.AvroIndexedRecordPartitioner.class)
              .groupingComparatorClass(JoinUtils.AvroPairGroupingComparator.class)
              .build();
      Schema outputUnionSchema = unionValueSchema("ExOutputUnion", outputSchemas);
      PType<GenericData.Record> outputUnion = Avros.generics(outputUnionSchema);
      TblFactory tblFactory = new TblFactory.Compute(config.outputTables, providerLists);
      PTable<Integer, GenericData.Record> reduced = mapside.groupByKey(opts)
              .combineValues(new ExCombiner(provider, keyType, interValueType, tblFactory))
              .parallelDo("merge",
                  new MergeRowsFn(tblFactory, outputUnionSchema),
                  Avros.tableOf(Avros.ints(), outputUnion));

      for (int i = 0; i < outputAggs.size(); i++) {
        int outputId = outputAggs.get(i).getOutputId();
        OutputConfig output = config.outputTables.get(outputId);
        AvroType<GenericData.Record> outType = Avros.generics(outputSchemas.get(i));
        PCollection<GenericData.Record> out = reduced.parallelDo(new FilterOutFn(outputId), outType);
        prepOutput(out, output, config.local);
      }
    }
    PipelineResult res = p.done();
    return res.succeeded() ? 0 : 1;
  }

  private void prepOutput(PCollection<GenericData.Record> out, AbstractOutputConfig output, boolean local) {
    if (local) {
      out.write(To.textFile(output.path));
    } else {
      AvroType<GenericData.Record> outType = (AvroType<GenericData.Record>) out.getPType();
      DatasetDescriptor dd = new DatasetDescriptor.Builder()
              .schema(outType.getSchema())
              .format(output.format)
              .location(output.path)
              .build();
      if (Datasets.exists(output.uri)) {
        Datasets.delete(output.uri);
      }
      Datasets.create(output.uri, dd);
      if ("avro".equals(output.format)) {
        out.write(To.avroFile(output.path), Target.WriteMode.APPEND);
      } else if ("parquet".equals(output.format)) {
        out.write(new AvroParquetFileTarget(output.path), Target.WriteMode.APPEND);
      } else {
        throw new IllegalArgumentException("Unsupported output format: " + output.format);
      }
    }
  }

  int build2(String arg) throws Exception {
    Build2Config config = ConfigHelper.parseBuild2Config(arg);
    Pipeline p = new MRPipeline(ExhibitTool.class, "BuildSupernova", getConf());

    Map<String, PCollection<GenericData.Record>> sources = Maps.newHashMap();
    for (Source2Config src : config.sources) {
      PCollection<GenericData.Record> pcol = ConfigHelper.getPCollection(p, src.uri, src.path);
      sources.put(src.name, pcol);
    }

    List<Schema> keySchemas = Lists.newArrayList();
    List<Schema> valueSchemas = Lists.newArrayList();
    List<List<PTable<GenericData.Record, Pair<Integer, GenericData.Record>>>> ptbls = Lists.newArrayList();
    List<List<SchemaProvider>> providerLists = Lists.newArrayList();
    List<Schema> outputSchemas = Lists.newArrayList();
    for (int i = 0; i < config.outputs.size(); i++) {
      BuildOutConfig out = config.outputs.get(i);
      List<Schema.Field> keyFields = Lists.newArrayList();
      List<Schema.Field> outputFields = Lists.newArrayList();
      List<PTable<GenericData.Record, Pair<Integer, GenericData.Record>>> ptbl = Lists.newArrayList();
      for (int j = 0; j < out.keys.size(); j++) {
        Schema fschema = AvroExhibit.getSchema(ObsDescriptor.FieldType.valueOf(out.keyTypes.get(j)));
        keyFields.add(new Schema.Field(out.keys.get(j), fschema, "", null));
        outputFields.add(new Schema.Field(out.keys.get(j), fschema, "", null));
      }
      Schema keySchema = Schema.createRecord("BuildKey" + i, "", "exhibit", false);
      keySchema.setFields(keyFields);
      keySchemas.add(keySchema);
      AvroType<GenericData.Record> keyType = Avros.generics(keySchema);
      List<SchemaProvider> providers = Lists.newArrayList();
      for (int j = 0; j < out.components.size(); j++) {
        ComponentConfig cmp = out.components.get(j);
        PCollection<GenericData.Record> src = sources.get(cmp.source);
        Schema inSchema = ((AvroType) src.getPType()).getSchema();
        SchemaProvider sp = null;
        if (cmp.embedded) {
          Map<String, String> cmpValues = cmp.getValues();
          List<Schema.Field> outFields = Lists.newArrayList();
          for (Map.Entry<String, String> e : cmpValues.entrySet()) {
            Schema.Field sf = inSchema.getField(e.getKey());
            outFields.add(new Schema.Field(e.getValue(), sf.schema(), sf.doc(), sf.defaultValue()));
          }
          Schema outSchema = Schema.createRecord("EmbeddedValue" + i + "_" + j, "", "exhibit", false);
          outSchema.setFields(outFields);
          sp = new SchemaProvider(ImmutableList.of(outSchema, outSchema));
        } else {
          Tbl tbl = cmp.createTbl();
          sp = tbl.getSchemas(new AvroObsDescriptor(inSchema), i, j);
        }
        for (Schema.Field sf : sp.get(1).getFields()) {
          // Copy fields to output
          outputFields.add(new Schema.Field(sf.name(), sf.schema(), sf.doc(), sf.defaultValue()));
        }
        valueSchemas.add(sp.get(0));
        PTable<GenericData.Record, Pair<Integer, GenericData.Record>> pt = src.parallelDo(
            new ComponentMapFn(j, cmp, keySchema, sp),
            Avros.tableOf(keyType, Avros.pairs(Avros.ints(), Avros.generics(sp.get(0)))));
        ptbl.add(pt);
        providers.add(sp);
      }
      Schema outputSchema = Schema.createRecord("ExhibitBuild" + i, "", "exhibit", false);
      outputSchema.setFields(outputFields);
      outputSchemas.add(outputSchema);
      ptbls.add(ptbl);
      providerLists.add(providers);
    }

    Schema interKeySchema = unionKeySchema("ExhibitBuildKey", keySchemas);
    Schema interValueSchema = unionValueSchema("ExhibitBuildValue", valueSchemas);
    SchemaProvider interProvider = new SchemaProvider(ImmutableList.of(interKeySchema, interValueSchema));
    PTable<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> merged = null;
    AvroType<GenericData.Record> keyType = Avros.generics(interKeySchema);
    AvroType<GenericData.Record> valueType = Avros.generics(interValueSchema);
    PTableType<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> itype = Avros.tableOf(
        Avros.pairs(keyType, Avros.ints()),
        Avros.pairs(Avros.ints(), valueType));
    for (int i = 0; i < ptbls.size(); i++) {
      for (int j = 0; j < ptbls.get(i).size(); j++) {
        PTable<Pair<GenericData.Record, Integer>, Pair<Integer, GenericData.Record>> union = ptbls.get(i).get(j).parallelDo(
            new SchemaMapFn(i, interProvider), itype);
        merged = (merged == null) ? union : merged.union(union);
      }
    }

    GroupingOptions opts = GroupingOptions.builder()
            .numReducers(config.parallelism)
            .partitionerClass(JoinUtils.AvroIndexedRecordPartitioner.class)
            .groupingComparatorClass(JoinUtils.AvroPairGroupingComparator.class)
            .build();

    Schema outputUnionSchema = unionValueSchema("ExBuildUnion", outputSchemas);
    PType<GenericData.Record> outputUnion = Avros.generics(outputUnionSchema);
    TblFactory tblFactory = new TblFactory.Build(config.outputs, providerLists);
    PTable<Integer, GenericData.Record> reduced = merged.groupByKey(opts)
            .combineValues(new ExCombiner(interProvider, keyType, valueType, tblFactory))
            .parallelDo("merge",
                    new MergeRowsFn(tblFactory, outputUnionSchema),
                    Avros.tableOf(Avros.ints(), outputUnion));

    for (int i = 0; i < config.outputs.size(); i++) {
      BuildOutConfig output = config.outputs.get(i);
      AvroType<GenericData.Record> outType = Avros.generics(outputSchemas.get(i));
      PCollection<GenericData.Record> out = reduced.parallelDo(new FilterOutFn(i), outType);
      prepOutput(out, output, false);
    }

    PipelineResult res = p.done();
    return res.succeeded() ? 0 : 1;
  }

  int build(String arg) throws Exception {
    BuildConfig config = ConfigHelper.parseBuildConfig(arg);
    Pipeline p = new MRPipeline(ExhibitTool.class, "BuildSupernova", getConf());

    List<Schema.Field> keySchemaFields = Lists.newArrayList();
    for (int i = 0; i < config.keys.size(); i++) {
      Schema fieldSchema = Schema.create(Schema.Type.valueOf(config.keyTypes.get(i)));
      List<Schema> optionals = Lists.newArrayList(fieldSchema, Schema.create(Schema.Type.NULL));
      Schema optional = Schema.createUnion(optionals);
      Schema.Field sf = new Schema.Field(config.keys.get(i), optional, "", null);
      keySchemaFields.add(sf);
    }
    Schema keySchema = Schema.createRecord("BuildKeySchema", "", "exhibit", false);
    keySchema.setFields(keySchemaFields);

    List<PCollection<GenericData.Record>> pcols = Lists.newArrayList();
    Set<Schema> schemas = Sets.newHashSet();
    for (SourceConfig src : config.sources) {
      PCollection<GenericData.Record> pcol = ConfigHelper.getPCollection(p, src.uri, src.path);
      pcols.add(pcol);
      Schema schema = ((AvroType) pcol.getPType()).getSchema();
      if (!src.drop.isEmpty()) {
        List<Schema.Field> keep = Lists.newArrayList();
        for (Schema.Field sf : schema.getFields()) {
          if (!src.drop.contains(sf.name())) {
            keep.add(new Schema.Field(sf.name(), sf.schema(), sf.doc(), sf.defaultValue()));
          }
        }
        schema = Schema.createRecord(schema.getName() + "_kept", schema.getDoc(), schema.getNamespace(), schema.isError());
        schema.setFields(keep);
      }
      src.setSchema(schema);
      schemas.add(schema);
    }

    // Hack to union the various schemas that will get processed together.
    Schema wrapper = unionValueSchema("ExhibitWrapper", Lists.newArrayList(schemas));
    AvroType<GenericData.Record> valueType = Avros.generics(wrapper);

    AvroType<Pair<Integer, GenericData.Record>> ssType = Avros.pairs(Avros.ints(), valueType);
    AvroType<GenericData.Record> keyType = Avros.generics(keySchema);
    PTableType<GenericData.Record, Pair<Integer, GenericData.Record>> tableType = Avros.tableOf(keyType, ssType);
    PTable<GenericData.Record, Pair<Integer, GenericData.Record>> union = null;
    for (int i = 0; i < config.sources.size(); i++) {
      SourceConfig src = config.sources.get(i);
      PCollection<GenericData.Record> in = pcols.get(i);
      KeyIndexFn<GenericData.Record> keyFn = new KeyIndexFn<GenericData.Record>(keyType, valueType, src, i);
      PTable<GenericData.Record, Pair<Integer, GenericData.Record>> keyed = in.parallelDo("src " + i, keyFn, tableType);
      if (union == null) {
        union = keyed;
      } else {
        union = union.union(keyed);
      }
    }
    MergeSchema ms = new MergeSchema(config.name, keySchema, config.sources, config.parallelism);
    PCollection<GenericData.Record> output = ms.apply(union);
    DatasetDescriptor dd = new DatasetDescriptor.Builder()
        .schema(((AvroType) output.getPType()).getSchema())
        .format(config.format)
        .location(config.path)
        .compressionType(config.compress)
        .build();
    if (Datasets.exists(config.uri) && config.writeMode == Target.WriteMode.OVERWRITE) {
      Datasets.delete(config.uri);
    }
    Datasets.create(config.uri, dd);
    if ("avro".equals(config.format)) {
      output.write(To.avroFile(config.path), config.writeMode);
    } else if ("parquet".equals(config.format)) {
      output.write(new AvroParquetFileTarget(config.path), config.writeMode);
    } else {
      throw new IllegalArgumentException("Unsupported output format: " + config.format);
    }
    PipelineResult res = p.done();
    return res.succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new ExhibitTool(), args);
    System.exit(rc);
  }
}
