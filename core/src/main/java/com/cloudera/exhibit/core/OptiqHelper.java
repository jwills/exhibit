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
package com.cloudera.exhibit.core;

import com.google.common.base.Preconditions;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class OptiqHelper implements Serializable {

  private transient OptiqConnection conn;
  private transient ModifiableSchema rootSchema;
  private String[] queries;

  public OptiqHelper() {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find Optiq Driver", e);
    }
  }

  public void initialize(List<? extends Table> tables, String[] queries) throws SQLException {
    if (conn != null) {
      conn.close();
    }
    this.rootSchema = new ModifiableSchema();
    for (int i = 0; i < tables.size(); i++) {
      rootSchema.put("T" + (i + 1), tables.get(i));
    }
    this.queries = Preconditions.checkNotNull(queries);
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    this.conn = connection.unwrap(OptiqConnection.class);
    conn.getRootSchema().add("X", rootSchema);
    conn.setSchema("X");
  }

  public String getLastQuery() {
    return queries[queries.length - 1];
  }

  public Statement newStatement() throws SQLException { return conn.createStatement(); }

  public void closeStatement(Statement stmt) {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        //TODO: log this
      }
    }
  }

  public ResultSet execute(Statement stmt) throws SQLException {
    try {
      for (int i = 0; i < queries.length - 1; i++) {
        Table tbl = ResultSetTable.create(stmt.executeQuery(queries[i]));
        rootSchema.put("TEMP" + (i + 1), tbl);
        rootSchema.put("LAST", tbl);
      }
      return stmt.executeQuery(queries[queries.length - 1]);
    } catch (SQLException e) {
      throw e;
    }
  }

  public void close() throws SQLException {
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }
}
