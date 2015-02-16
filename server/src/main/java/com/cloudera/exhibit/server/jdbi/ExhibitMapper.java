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
package com.cloudera.exhibit.server.jdbi;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public class ExhibitMapper implements ResultSetMapper<Exhibit> {

  private final ObsDescriptor id;

  public ExhibitMapper(String id) {
    this.id = SimpleObsDescriptor.of(id, ObsDescriptor.FieldType.STRING);
  }

  @Override
  public Exhibit map(int index, ResultSet rs, StatementContext ctxt) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    String idValue = null;
    Map<String, Frame> frames = Maps.newHashMap();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      if (md.getColumnType(i) == Types.ARRAY) {
        frames.put(md.getColumnLabel(i), mapFrame(rs.getArray(i).getResultSet()));
      } else if (id.get(0).name.equals(md.getColumnLabel(i))) {
        idValue = rs.getString(i);
      }
    }
    return new SimpleExhibit(SimpleObs.of(id, idValue), frames);
  }

  public Frame mapFrame(ResultSet rs) throws SQLException {
    List<Obs> obs = Lists.newArrayList();
    ObsDescriptor desc = fromMetaData(rs.getMetaData());
    while (rs.next()) {
      List<Object> values = Lists.newArrayList();
      for (int i = 0; i < desc.size(); i++) {
        ObsDescriptor.Field f = desc.get(i);
        // TODO: should do stronger type checking here
        values.add(rs.getObject(f.name));
      }
      obs.add(new SimpleObs(desc, values));
    }
    return new SimpleFrame(desc, obs);
  }

  private Map<Integer, ObsDescriptor.FieldType> SQL_TO_FIELD_TYPE = ImmutableMap.<Integer, ObsDescriptor.FieldType>builder()
          .put(Types.INTEGER, ObsDescriptor.FieldType.INTEGER)
          .put(Types.BIGINT, ObsDescriptor.FieldType.LONG)
          .put(Types.VARCHAR, ObsDescriptor.FieldType.STRING)
          .put(Types.CHAR, ObsDescriptor.FieldType.STRING)
          .put(Types.BOOLEAN, ObsDescriptor.FieldType.BOOLEAN)
          .put(Types.DOUBLE, ObsDescriptor.FieldType.DOUBLE)
          .put(Types.FLOAT, ObsDescriptor.FieldType.FLOAT)
          .build();

  private ObsDescriptor fromMetaData(ResultSetMetaData metaData) throws SQLException {
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String name = metaData.getColumnLabel(i);
      ObsDescriptor.FieldType type = SQL_TO_FIELD_TYPE.get(metaData.getColumnType(i));
      if (type != null) {
        fields.add(new ObsDescriptor.Field(name, type));
      } else {
        // TODO: log this
        System.out.println("Could not handle type for name " + name + ": " + metaData.getColumnTypeName(i));
      }
    }
    return new SimpleObsDescriptor(fields);
  }
}
