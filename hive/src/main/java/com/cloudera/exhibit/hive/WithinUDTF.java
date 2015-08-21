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

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "within_table",
    value = "_FUNC_(query_str, ...) - Yo dawg, I heard you liked SQL. So we put SQL in your SQL, so you can " +
            "query while you query.")
public class WithinUDTF extends GenericUDTF {

  private Calculator calculator;
  private transient Exhibit exhibit;
  private transient Object[] results;

  public WithinUDTF() {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= 1) {
      throw new UDFArgumentLengthException("The 'within' function takes at least two arguments");
    }

    this.calculator = HiveUtils.getCalculator(args[0]);
    this.exhibit = HiveUtils.getExhibit(args);
    ObsDescriptor od = calculator.initialize(exhibit.descriptor());

    this.results = new Object[od.size()];
    return (StructObjectInspector) HiveUtils.fromDescriptor(od, true);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    for (int i = 1; i < args.length; i++) {
      HiveUtils.update(exhibit, "t" + i, args[i]);
    }
    Iterable<Obs> res = calculator.apply(exhibit);
    for (Obs obs : res) {
      for (int i = 0; i < results.length; i++) {
        results[i] = HiveUtils.asHiveType(obs.get(i));
      }
      forward(results);
    }
  }

  @Override
  public void close() throws HiveException {
    calculator.cleanup();
    calculator = null;
  }
}
