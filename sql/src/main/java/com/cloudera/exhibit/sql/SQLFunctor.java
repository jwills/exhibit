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
package com.cloudera.exhibit.sql;

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.simple.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Table;

import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SQLFunctor implements Serializable, Functor {

  private transient ModifiableSchema rootSchema;
  private transient CalciteConnection conn;
  private transient List<PreparedStatement> stmts;
  private final String[] queries;

  private final String resultFrame;
  public static final String DEFAULT_RESULT_FRAME = "LAST";

  public static SQLFunctor create(String resultFrame, ObsDescriptor res, String sqlCode) {
    if (sqlCode == null) {
      return null;
    }
    List<String> ret = Lists.newArrayList();
    //TODO: sql comment filtering
    for (String q : Splitter.on(';').trimResults().omitEmptyStrings().split(sqlCode)) {
      ret.add(q);
    }
    SQLFunctor sc = new SQLFunctor(resultFrame, ret.toArray(new String[0]));
    return sc;
  }
  public static SQLFunctor create(ObsDescriptor res, String sqlCode) {
    return create(DEFAULT_RESULT_FRAME, res, sqlCode);
  }

  public SQLFunctor(String resultFrame, String[] queries){
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find Calcite Driver", e);
    }
    this.queries = Preconditions.checkNotNull(queries);
    this.resultFrame = resultFrame;

  }
  public SQLFunctor(String[] queries) {
    this(DEFAULT_RESULT_FRAME, queries);
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor descriptor) {
    this.rootSchema = new ModifiableSchema();
    rootSchema.getTableMap().put("ATTRS", new FrameTable(descriptor.attributes()));
    for (Map.Entry<String, ObsDescriptor> e : descriptor.frames().entrySet()) {
      rootSchema.getTableMap().put(e.getKey().toUpperCase(), new FrameTable(e.getValue()));
    }
    for (Map.Entry<String, FieldType> e : descriptor.vectors().entrySet()) {
      rootSchema.getTableMap().put(e.getKey().toUpperCase(), new VectorTable(e.getValue()));
    }
    try {
      this.conn = newConnection();
      this.stmts = Lists.newArrayList();
      for (int i = 0; i < queries.length - 1; i++) {
        PreparedStatement ps = conn.prepareStatement(queries[i]);
        Table tbl = ResultSetTable.create(ps.executeQuery());
        rootSchema.getTableMap().put("TEMP" + (i + 1), tbl);
        rootSchema.getTableMap().put("LAST", tbl);
        stmts.add(ps);
      }
      PreparedStatement result = conn.prepareStatement(queries[queries.length - 1]);
      stmts.add(result);
      return SimpleExhibitDescriptor.of(resultFrame, fromResultSet(result.executeQuery()).descriptor());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    try {
      rootSchema = null;
      if (stmts != null) {
        for (PreparedStatement stmt : stmts) {
          stmt.close();
        }
        this.stmts = null;
      }
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private CalciteConnection newConnection() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection oconn = connection.unwrap(CalciteConnection.class);
    oconn.getRootSchema().add("X", rootSchema);
    oconn.setSchema("X");
    return oconn;
  }

  @Override
  public Exhibit apply(Exhibit exhibit) {
    rootSchema.getFrame("ATTRS").updateFrame(new SimpleFrame(ImmutableList.of(exhibit.attributes())));
    for (Map.Entry<String, Frame> e : exhibit.frames().entrySet()) {
      rootSchema.getFrame(e.getKey().toUpperCase()).updateFrame(e.getValue());
    }
    for (Map.Entry<String, Vector> e : exhibit.vectors().entrySet()) {
      rootSchema.getVector(e.getKey().toUpperCase()).updateVector(e.getValue());
    }
    conn.getRootSchema().add("X", rootSchema);
    try {
      conn.setSchema("X");
      for (int i = 0; i < queries.length - 1; i++) {
        Table tbl = ResultSetTable.create(stmts.get(i).executeQuery());
        rootSchema.getTableMap().put("TEMP" + (i + 1), tbl);
        rootSchema.getTableMap().put("LAST", tbl);
      }
      return SimpleExhibit.of(resultFrame, fromResultSet(stmts.get(queries.length - 1).executeQuery()));
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Frame fromResultSet(ResultSet rs) throws SQLException {
    ObsDescriptor desc = fromMetadata(rs.getMetaData());
    List<Obs> obs = Lists.newArrayList();
    while (rs.next()) {
      List<Object> values = Lists.newArrayListWithExpectedSize(desc.size());
      for (int i = 1; i <= desc.size(); i++) {
        values.add(rs.getObject(i));
      }
      obs.add(new SimpleObs(desc, values));
    }
    return new SimpleFrame(desc, obs);
  }

  private ObsDescriptor fromMetadata(ResultSetMetaData md) throws SQLException {
    List<ObsDescriptor.Field> fields = Lists.newArrayListWithExpectedSize(md.getColumnCount());
    for (int i = 0; i < md.getColumnCount(); i++) {
      String name = md.getColumnLabel(i + 1).toLowerCase(Locale.ENGLISH);
      fields.add(new ObsDescriptor.Field(name, TypeUtils.getFieldTypeForSQLType(md.getColumnType(i + 1))));
    }
    return new SimpleObsDescriptor(fields);
  }
}
