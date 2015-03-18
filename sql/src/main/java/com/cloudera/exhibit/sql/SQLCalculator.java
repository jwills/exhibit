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
package com.cloudera.exhibit.sql;

import com.cloudera.exhibit.core.FrameCalculator;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SQLCalculator implements Serializable, FrameCalculator {

  private transient ModifiableSchema rootSchema;
  private transient OptiqConnection conn;
  private transient List<PreparedStatement> stmts;
  private final String[] queries;

  public SQLCalculator(String[] queries) {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find Optiq Driver", e);
    }
    this.queries = Preconditions.checkNotNull(queries);
  }

  private void initialize(ExhibitDescriptor descriptor) throws SQLException {
    this.rootSchema = new ModifiableSchema();
    for (Map.Entry<String, ObsDescriptor> e : descriptor.frames().entrySet()) {
      rootSchema.getTableMap().put(e.getKey().toUpperCase(), new FrameTable(e.getValue()));
    }
    this.conn = newConnection();
    this.stmts = Lists.newArrayList();
    for (int i = 0; i < queries.length - 1; i++) {
      PreparedStatement ps = conn.prepareStatement(queries[i]);
      Table tbl = ResultSetTable.create(ps.executeQuery());
      rootSchema.getTableMap().put("TEMP" + (i + 1), tbl);
      rootSchema.getTableMap().put("LAST", tbl);
      stmts.add(ps);
    }
    stmts.add(conn.prepareStatement(queries[queries.length - 1]));
  }

  private OptiqConnection newConnection() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection oconn = connection.unwrap(OptiqConnection.class);
    oconn.getRootSchema().add("X", rootSchema);
    oconn.setSchema("X");
    return oconn;
  }

  @Override
  public Frame apply(Exhibit exhibit) {
    if (rootSchema == null) {
      try {
        initialize(exhibit.descriptor());
      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    for (Map.Entry<String, Frame> e : exhibit.frames().entrySet()) {
      rootSchema.getFrame(e.getKey().toUpperCase()).updateFrame(e.getValue());
    }
    conn.getRootSchema().add("X", rootSchema);
    try {
      conn.setSchema("X");
      for (int i = 0; i < queries.length - 1; i++) {
        Table tbl = ResultSetTable.create(stmts.get(i).executeQuery());
        rootSchema.getTableMap().put("TEMP" + (i + 1), tbl);
        rootSchema.getTableMap().put("LAST", tbl);
      }
      return fromResultSet(stmts.get(queries.length - 1).executeQuery());
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
