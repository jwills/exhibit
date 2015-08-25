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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Functor;
import com.cloudera.exhibit.core.Vec;
import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ExhibitUDF extends GenericUDF {

  private Functor functor;
  private ObjectInspector[] inspectors;
  private StructObjectInspector resOI;
  private Object[] values;

  private transient Exhibit exhibit;

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length <= 1) {
      throw new UDFArgumentLengthException("The 'exhibit' function takes at least two arguments");
    }
    this.inspectors = args;
    this.functor = HiveUtils.getFunctor(args[0]);
    this.exhibit = HiveUtils.getExhibit(args);
    ExhibitDescriptor ed = functor.initialize(exhibit.descriptor());
    this.resOI = HiveUtils.fromDescriptor(ed);
    this.values = new Object[resOI.getAllStructFieldRefs().size()];
    return resOI;
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    HiveUtils.update(exhibit, inspectors, args);
    Exhibit res = functor.apply(exhibit);
    for (int i = 0; i < resOI.getAllStructFieldRefs().size(); i++) {
      StructField sf = resOI.getAllStructFieldRefs().get(i);
      if (res.frames().containsKey(sf.getFieldName())) {
        Frame f = res.frames().get(sf.getFieldName());
        Object[] fa = new Object[f.size()];
        for (int j = 0; j < f.size(); j++) {
          Object[] oa = new Object[f.descriptor().size()];
          for (int k = 0; k < f.descriptor().size(); k++) {
            oa[k] = f.get(j).get(k);
          }
          fa[j] = oa;
        }
        values[i] = fa;
      } else if (res.vectors().containsKey(sf.getFieldName())) {
        Vec v = res.vectors().get(sf.getFieldName());
        Object[] va = new Object[v.size()];
        for (int j = 0; j < v.size(); j++) {
          va[j] = HiveUtils.asHiveType(v.get(j));
        }
        values[i] = va;
      } else {
        int attr = res.attributes().descriptor().indexOf(sf.getFieldName());
        if (attr >= 0) {
          values[i] = HiveUtils.asHiveType(res.attributes().get(attr));
        }
      }
    }
    return values;
  }

  @Override
  public String getDisplayString(String[] args) {
    assert (args.length > 1);
    return "exhibit(" + Joiner.on(',').join(args) + ")";
  }
}
