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
package com.cloudera.exhibit.udtf;

import com.google.common.collect.Lists;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Description(name = "within",
    value = "_FUNC_(query_str, ...) - Yo dawg, I heard you liked SQL. So we put SQL in your SQL, so you can " +
            "query while you query.")
public class WithinUDTF extends GenericUDTF {

  private String[] queries;
  private transient List<HiveTable> tables;
  private transient HiveSchema hiveSchema;
  private transient OptiqConnection conn;
  private transient Object[] results;

  public WithinUDTF() {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find Optiq Driver", e);
    }
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= 1) {
      throw new UDFArgumentLengthException("The 'within' function takes at least two arguments");
    }

    ObjectInspector first = args[0];
    if (ObjectInspectorUtils.isConstantObjectInspector(first)) {
      Object wcv = ObjectInspectorUtils.getWritableConstantValue(first);

      if (first instanceof StringObjectInspector) {
        queries = new String[] { wcv.toString() };
      } else if (first instanceof ListObjectInspector) {
        ListObjectInspector lOI = (ListObjectInspector) first;
        int len = lOI.getListLength(wcv);
        if (lOI.getListElementObjectInspector() instanceof StringObjectInspector) {
          StringObjectInspector strOI = (StringObjectInspector) lOI.getListElementObjectInspector();
          queries = new String[len];
          for (int i = 0; i < len; i++) {
            queries[i] = strOI.getPrimitiveJavaObject(lOI.getListElement(wcv, i));
          }
        } else {
          throw new UDFArgumentException("Array of queries must be all strings");
        }
      }
    } else {
      throw new UDFArgumentException("First argument must be a constant string or array of strings");
    }

    this.tables = Lists.newArrayList();
    this.hiveSchema = new HiveSchema();
    //TODO: table aliases, map structures as indexed tables
    for (int i = 1; i < args.length; i++) {
      ObjectInspector oi = args[i];
      if (oi.getCategory() == ObjectInspector.Category.LIST) {
        ListObjectInspector loi = (ListObjectInspector) oi;
        ObjectInspector elOI = loi.getListElementObjectInspector();
        if (elOI.getCategory() == ObjectInspector.Category.STRUCT ||
            elOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
          Type tableType = Object[].class;
          if (elOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            tableType = ((PrimitiveObjectInspector) elOI).getJavaPrimitiveClass();
          }
          HiveTable tbl = new HiveTable(tableType, loi);
          tables.add(tbl);
          hiveSchema.put("T" + i, tbl);
        } else {
          throw new UDFArgumentException("Only arrays of structs/primitives are supported at this time");
        }
      } else {
        throw new UDFArgumentException("Only arrays of structs/primitives are supported at this time");
      }
    }

    Statement stmt;
    try {
      stmt = initializeConnection().createStatement();
    } catch (SQLException e) {
      throw new UDFArgumentException("Error setting up Optiq connection: " + e);
    }

    try {
      StructObjectInspector res = fromMetaData(executeQueries(stmt).getMetaData());
      stmt.close();
      return res;
    } catch (SQLException e) {
      throw new IllegalStateException("Schema validation query failure: " + e.getMessage(), e);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e2) {
          throw new IllegalStateException("Error attempting to close Statement", e2);
        }
      }
    }
  }

  private StructObjectInspector fromMetaData(ResultSetMetaData metadata) throws UDFArgumentException, SQLException {
    int colCount = metadata.getColumnCount();
    if (colCount == 0) {
      throw new UDFArgumentException("No columns returned from query: " + queries[queries.length - 1]);
    }

    this.results = new Object[colCount];
    List<String> names = Lists.newArrayList();
    List<ObjectInspector> inspectors = Lists.newArrayList();
    for (int i = 1; i <= colCount; i++) {
      names.add(metadata.getColumnName(i));
      ObjectInspector oi = TypeUtils.getObjectInspectorForSQLType(metadata.getColumnType(i));
      if (oi == null) {
        throw new UDFArgumentException("Unknown column type in result: " + metadata.getColumnTypeName(i));
      }
      inspectors.add(oi);
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
  }

  private Connection initializeConnection() throws SQLException {
    if (conn != null) {
      conn.close();
    }
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    this.conn = connection.unwrap(OptiqConnection.class);
    conn.getRootSchema().add("X", hiveSchema);
    conn.setSchema("X");
    return conn;
  }

  private ResultSet executeQueries(Statement stmt) throws SQLException {
    for (int i = 0; i < queries.length - 1; i++) {
      Table tbl = TempTable.fromResultSet(stmt.executeQuery(queries[i]));
      hiveSchema.put("TEMP" + (i + 1), tbl);
      hiveSchema.put("LAST", tbl);
    }
    return stmt.executeQuery(queries[queries.length - 1]);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    for (int i = 1; i < args.length; i++) {
      tables.get(i - 1).updateValues(args[i]);
    }
    Statement stmt = null;
    try {
      stmt = initializeConnection().createStatement();
      ResultSet rs = executeQueries(stmt);
      while (rs.next()) {
        for (int i = 0; i < results.length; i++) {
          results[i] = rs.getObject(i + 1);
        }
        forward(results);
      }
    } catch (SQLException e) {
      throw new HiveException("Error processing SQL query", e);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e2) {
          throw new HiveException("Error attempting to close Statement object", e2);
        }
      }
    }
  }

  @Override
  public void close() throws HiveException {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      throw new HiveException("SQL exception closing connection", e);
    }
  }
}
