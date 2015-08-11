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
package com.cloudera.exhibit.hive;

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.sql.SQLFunctor;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Map;

@Description(name = "within",
    value = "_FUNC_(query_str, ...) - Yo dawg, I heard you liked SQL. So we put SQL in your SQL, so you can " +
            "query while you query.")
public class WithinUDTF extends GenericUDTF {

  private SQLFunctor functor;
  private transient Exhibit exhibit;
  private transient Object[] results;

  public WithinUDTF() {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= 1) {
      throw new UDFArgumentLengthException("The 'within' function takes at least two arguments");
    }

    ObjectInspector first = args[0];
    String[] queries = HiveUtils.getQueries(first);

    Map<String, Frame> frames = Maps.newHashMap();
    for (int i = 1; i < args.length; i++) {
      frames.put("T" + i, HiveUtils.getHiveFrame(args[i]));
    }
    Map<String, Vector> vectors = Maps.newHashMap(); // TODO: implement
    this.exhibit = new SimpleExhibit(Obs.EMPTY, frames, vectors);
    this.functor = new SQLFunctor(queries);
    ExhibitDescriptor ed = functor.initialize(exhibit.descriptor());
    ObsDescriptor     od = ed.frames().get(SQLFunctor.DEFAULT_RESULT_FRAME);
    this.results = new Object[od.size()];
    return (StructObjectInspector) HiveUtils.fromDescriptor(od, true);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    for (int i = 1; i < args.length; i++) {
      ((HiveFrame) exhibit.frames().get("T" + i)).updateValues(args[i]);
    }
    Exhibit result    = functor.apply(exhibit);
    Frame res = result.frames().get(SQLFunctor.DEFAULT_RESULT_FRAME);
    for (Obs obs : res) {
      for (int i = 0; i < results.length; i++) {
        results[i] = HiveUtils.asHiveType(obs.get(i));
      }
      forward(results);
    }
  }

  @Override
  public void close() throws HiveException {
    functor.cleanup();
    functor = null;
  }
}
