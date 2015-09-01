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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectAllUDAF extends AbstractGenericUDAFResolver {

  private static final Logger LOG = LoggerFactory.getLogger(CollectAllUDAF.class);

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfo) throws SemanticException {
    if (typeInfo.length != 1) {
      throw new UDFArgumentException("Only one argument expected to collect_all method");
    }
    if (typeInfo[0].getCategory() == ObjectInspector.Category.LIST) {
      LOG.info("Using CollectArrayEvaluator");
      return new AbstractCollectArrayEvaluator.Lists();
    }
    LOG.info("Using CollectEvaluator");
    return new AbstractCollectEvaluator.Lists();
  }
}
