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
package com.cloudera.exhibit.renjin;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Column;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.collect.ImmutableList;
import org.renjin.eval.Session;
import org.renjin.eval.SessionBuilder;
import org.renjin.parser.RParser;
import org.renjin.sexp.DoubleArrayVector;
import org.renjin.sexp.Environment;
import org.renjin.sexp.IntArrayVector;
import org.renjin.sexp.ListVector;
import org.renjin.sexp.LogicalArrayVector;
import org.renjin.sexp.Null;
import org.renjin.sexp.SEXP;
import org.renjin.sexp.StringArrayVector;
import org.renjin.sexp.StringVector;

import java.io.Serializable;
import java.util.Map;

public class RenjinCalculator implements Serializable, Calculator {

  private static StringArrayVector DF_CLASS = new StringArrayVector("data.frame");

  private String src;

  private transient Session session;
  private transient SEXP parsed;

  public RenjinCalculator(String src) {
    this.src = src + "\n";
    this.parsed = RParser.parseSource(this.src);
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor descriptor) {
    if (parsed == null) {
      parsed = RParser.parseSource(src);
    }
    if (session == null) {
      session = new SessionBuilder().withDefaultPackages().build();
    }
    //TODO: trivial exhibit impl
    return null;
  }

  @Override
  public void cleanup() {
    session.close();
  }

  @Override
  public Iterable<Obs> apply(Exhibit exhibit) {
    Environment env = session.getTopLevelContext().getEnvironment();
    env.clear();

    Obs attrs = exhibit.attributes();
    for (int i = 0; i < attrs.descriptor().size(); i++) {
      ObsDescriptor.Field f = attrs.descriptor().get(i);
      env.setVariable(f.name, toSEXP(attrs.get(i), f.type));
    }
    for (Map.Entry<String, Frame> e : exhibit.frames().entrySet()) {
      ListVector.NamedBuilder nb = ListVector.newNamedBuilder();
      ObsDescriptor od = e.getValue().descriptor();
      for (int i = 0; i < od.size(); i++) {
        ObsDescriptor.Field f = od.get(i);
        nb.add(f.name, toSEXP(e.getValue().$(i), f.type));
      }
      nb.setAttribute("class", DF_CLASS);
      env.setVariable(e.getKey(), nb.build());
    }

    SEXP res = session.getTopLevelContext().evaluate(parsed).force(session.getTopLevelContext());
    if (res instanceof ListVector) {
      return new ListVectorFrame((ListVector) res);
    }
    return ImmutableList.of();
  }

  static SEXP toSEXP(Object obj, ObsDescriptor.FieldType ftype) {
    if (obj == null) {
      return Null.INSTANCE;
    }
    switch (ftype) {
      case BOOLEAN:
        LogicalArrayVector.Builder lb = new LogicalArrayVector.Builder();
        if (obj instanceof Column) {
          Column c = (Column) obj;
          for (int i = 0; i < c.size(); i++) {
            Boolean b = (Boolean) c.get(i);
            if (b == null) {
              lb.addNA();
            } else {
              lb.add(b);
            }
          }
        } else {
          Boolean b = (Boolean) obj;
          if (b == null) {
            lb.addNA();
          } else {
            lb.add(b);
          }
        }
        return lb.build();
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        DoubleArrayVector.Builder db = new DoubleArrayVector.Builder();
        if (obj instanceof Column) {
          Column c = (Column) obj;
          for (int i = 0; i < c.size(); i++) {
            Number n = (Number) c.get(i);
            db.add(n == null? DoubleArrayVector.NA : n);
          }
        } else {
          Number n = (Number) obj;
          db.add(n == null? DoubleArrayVector.NA : n);
        }
        return db.build();
      case INTEGER:
      case LONG:
        IntArrayVector.Builder ib = new IntArrayVector.Builder();
        if (obj instanceof Column) {
          Column c = (Column) obj;
          for (int i = 0; i < c.size(); i++) {
            Number n = (Number) c.get(i);
            ib.add(n == null? IntArrayVector.NA : n);
          }
        } else {
          Number n = (Number) obj;
          ib.add(n == null? IntArrayVector.NA : n);
        }
        return ib.build();
      case STRING:
        StringArrayVector.Builder sb = new StringVector.Builder();
        if (obj instanceof Column) {
          Column c = (Column) obj;
          for (int i = 0; i < c.size(); i++) {
            String s = (String) c.get(i);
            sb.add(s == null? StringVector.NA : s);
          }
        } else {
          String s = (String) obj;
          sb.add(s == null? StringVector.NA : s);
        }
        return sb.build();
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnsupportedOperationException("Cannot convert field type in R: " + ftype);
    }
  }
}
