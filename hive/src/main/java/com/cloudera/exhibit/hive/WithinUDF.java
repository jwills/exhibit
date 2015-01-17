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
package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.OptiqHelper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class WithinUDF extends GenericUDF {

  private OptiqHelper helper;
  private transient List<HiveTable> tables;

  public WithinUDF() {
    this.helper = new OptiqHelper();
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= 1) {
      throw new UDFArgumentLengthException("The 'within' function takes at least two arguments");
    }

    ObjectInspector first = args[0];
    String[] queries = HiveUtils.getQueries(first);

    this.tables = Lists.newArrayList();
    for (int i = 1; i < args.length; i++) {
      HiveTable tbl = HiveUtils.getHiveTable(args[i]);
      tables.add(tbl);
    }

    try {
      helper.initialize(tables, queries);
    } catch (SQLException e) {
      throw new IllegalStateException("Optiq initialization error", e);
    }

    try {
      StructObjectInspector res = HiveUtils.fromMetaData(helper);
      return ObjectInspectorFactory.getStandardListObjectInspector(res);
    } catch (SQLException e) {
      throw new IllegalStateException("Schema validation query failure: " + e.getMessage(), e);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    for (int i = 1; i < args.length; i++) {
      tables.get(i - 1).updateValues(args[i].get());
    }
    Statement stmt = null;
    try {
      stmt = helper.newStatement();
      ResultSet rs = helper.execute(stmt);
      List result = Lists.newArrayList();
      while (rs.next()) {
        Object[] ret = new Object[rs.getMetaData().getColumnCount()];
        for (int i = 0; i < ret.length; i++) {
          ret[i] = HiveUtils.asHiveType(rs.getObject(i + 1));
        }
        result.add(ret);
      }
      return result;
    } catch (SQLException e) {
      throw new HiveException("Error processing SQL query", e);
    } finally {
      helper.closeStatement(stmt);
    }
  }

  @Override
  public String getDisplayString(String[] args) {
    assert (args.length > 1);
    return "within(" + Joiner.on(',').join(args) + ")";
  }
}
