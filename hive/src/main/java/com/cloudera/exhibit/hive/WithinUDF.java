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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class WithinUDF extends GenericUDF {

  private Calculator calculator;
  private ObjectInspector[] inspectors;

  private transient Exhibit exhibit;

  public WithinUDF() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= 1) {
      throw new UDFArgumentLengthException("The 'within' function takes at least two arguments");
    }
    this.inspectors = args;
    this.calculator = HiveUtils.getCalculator(args[0]);
    this.exhibit = HiveUtils.getExhibit(args);
    ObsDescriptor od = calculator.initialize(exhibit.descriptor());
    return HiveUtils.fromDescriptor(od, false);
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    HiveUtils.update(exhibit, inspectors, args);
    return getResult(Iterables.getOnlyElement(calculator.apply(exhibit)));
  }

  private Object getResult(Obs obs) {
    if (obs.descriptor().size() == 1) {
      return HiveUtils.asHiveType(obs.get(0));
    } else {
      List<Object> values = Lists.newArrayListWithExpectedSize(obs.descriptor().size());
      for (int i = 0; i < obs.descriptor().size(); i++) {
        values.add(HiveUtils.asHiveType(obs.get(i)));
      }
      return values;
    }
  }

  @Override
  public String getDisplayString(String[] args) {
    assert (args.length > 1);
    return "within(" + Joiner.on(',').join(args) + ")";
  }
}
