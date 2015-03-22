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
package com.cloudera.exhibit.javascript;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Exhibits;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsCalculator;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

public class JSCalculator implements ObsCalculator {

  static {
    ContextFactory.initGlobal(new ExhibitContextFactory());
  }

  private final String src;
  private final boolean hasReturn;

  private ObsDescriptor descriptor;

  private Context ctx;
  private Scriptable scope;
  private Script script;
  private Function func;

  public JSCalculator(ObsDescriptor descriptor, String src) throws ScriptException {
    this.src = src;
    this.hasReturn = src.contains("return");
    this.descriptor = descriptor;
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor ed) {
    ctx = Context.enter();
    ctx.setClassShutter(new ClassShutter() {
      @Override
      public boolean visibleToScripts(String className) {
        return className.startsWith("com.cloudera.exhibit");
      }
    });
    this.scope = ctx.initStandardObjects(null, true);
    if (hasReturn) {
      this.func = ctx.compileFunction(scope, "function() {" + src + "}", "<cmd>", 1, null);
    } else {
      this.script = ctx.compileString(src, "<cmd>", 1, null);
    }
    if (this.descriptor == null) {
      // need to eval the script with a default Exhibit
      Exhibit defaults = Exhibits.defaultValues(ed);
      this.descriptor = apply(defaults).descriptor();
    }
    return descriptor;
  }

  @Override
  public Obs apply(Exhibit exhibit) {
    Scriptable exhibitScope = ctx.newObject(scope);
    exhibitScope.setPrototype(scope);
    exhibitScope.setParentScope(null);

    Obs attr = exhibit.attributes();
    for (int i = 0; i < attr.descriptor().size(); i++) {
      exhibitScope.put(attr.descriptor().get(i).name, exhibitScope, attr.get(i));
    }
    for (Map.Entry<String, Frame> e : exhibit.frames().entrySet()) {
      exhibitScope.put(e.getKey(), exhibitScope, new ScriptableFrame(e.getValue()));
    }

    Object res;
    if (hasReturn) {
      res = func.call(ctx, exhibitScope, null, new Object[0]);
    } else {
      res = script.exec(ctx, exhibitScope);
    }
    return toObs(res);
  }

  Obs toObs(Object res) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    } else if (res instanceof Map) {
      Map<String, Object> mres = (Map<String, Object>) res;
      List<ObsDescriptor.Field> fields = Lists.newArrayList();
      List<Object> values = Lists.newArrayList();
      for (String key : Sets.newTreeSet(mres.keySet())) {
        Object val = mres.get(key);
        ObsDescriptor.FieldType ft = null;
        if (val == null) {
          throw new IllegalStateException("Null value for key: " + key);
        } else if (val instanceof Number) {
          ft = ObsDescriptor.FieldType.DOUBLE;
          val = ((Number) val).doubleValue();
        } else if (val instanceof String) {
          ft = ObsDescriptor.FieldType.STRING;
          val = val.toString();
        } else if (val instanceof Boolean) {
          ft = ObsDescriptor.FieldType.BOOLEAN;
        }
        fields.add(new ObsDescriptor.Field(key, ft));
        values.add(val);
      }
      return new SimpleObs(new SimpleObsDescriptor(fields), values);
    } else if (res instanceof Number) {
      return SimpleObs.of(SimpleObsDescriptor.of("res", ObsDescriptor.FieldType.DOUBLE), ((Number) res).doubleValue());
    } else if (res instanceof String) {
      return SimpleObs.of(SimpleObsDescriptor.of("res", ObsDescriptor.FieldType.STRING), res.toString());
    } else if (res instanceof Boolean) {
      return SimpleObs.of(SimpleObsDescriptor.of("res", ObsDescriptor.FieldType.BOOLEAN), res);
    } else {
      throw new IllegalStateException("Unsupported result type: " + res);
    }
  }

  @Override
  public void cleanup() {
    Context.exit();
  }
}
