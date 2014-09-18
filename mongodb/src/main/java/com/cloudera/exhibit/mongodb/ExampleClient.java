/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.exhibit.mongodb;

import com.cloudera.exhibit.core.OptiqHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class ExampleClient {
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: ExampleClient <db> <collection> <TABLE KEYS> <EXHIBIT SQL FILE>");
      System.exit(1);
    }
    MongoClient client = new MongoClient();
    DB db = client.getDB(args[0]);
    DBCollection collection = db.getCollection(args[1]);
    String[] keys = args[2].split(",");
    String[] queries = Files.readLines(new File(args[3]), Charsets.UTF_8).toArray(new String[0]);
    OptiqHelper helper = new OptiqHelper();
    DBCursor cursor = collection.find();
    while (cursor.hasNext()) {
      DBObject base = cursor.next();
      List<BSONTable> tables = Lists.newArrayList();
      for (String key : keys) {
        tables.add(BSONTable.create((List) base.get(key)));
      }
      helper.initialize(tables, queries);
      Statement stmt = null;
      try {
        stmt = helper.newStatement();
        ResultSet rs = helper.execute(stmt);
        while (rs.next()) {
          List<Object> res = Lists.newArrayList();
          for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            res.add(rs.getObject(i + 1));
          }
          System.out.println(Joiner.on(',').join(res));
        }
      } finally {
        helper.closeStatement(stmt);
      }
    }
    cursor.close();
    client.close();
  }
}
