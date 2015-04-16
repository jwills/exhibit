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
package com.cloudera.exhibit.etl.config;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.etl.tbl.SumTbl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericData;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AggConfig implements Serializable {

  public GenericData.Record init(GenericData.Record value) {
    return value;
  }

  public GenericData.Record merge(GenericData.Record merged, GenericData.Record second) {
    return (GenericData.Record) SumTbl.add(merged, second, second.getSchema());
  }

  public GenericData.Record finalize(GenericData.Record value) {
    return value;
  }

  public enum Type {
    SUM,
  }

  public Type type = Type.SUM;
  public Map<String, Object> options = Maps.newHashMap();
  public FrameConfig frame = null;
  public List<String> keys = Lists.newArrayList();
  public Map<String, String> values = Maps.newHashMap();

  public Calculator getCalculator() {
    if (frame == null) {
      throw new IllegalStateException("Invalid AggConfig: no frame specified");
    }
    return frame.getCalculator();
  }

  public ObsDescriptor getFrameDescriptor(ExhibitDescriptor ed) {
    if (frame != null) {
      Calculator c = frame.getCalculator();
      return c.initialize(ed);
    }
    throw new IllegalStateException("Invalid AggConfig: no frame specified");
  }
}
