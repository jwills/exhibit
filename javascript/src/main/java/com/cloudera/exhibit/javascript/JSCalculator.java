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
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsCalculator;
import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;

import javax.script.ScriptException;

public class JSCalculator implements ObsCalculator {

  static {
    ContextFactory.initGlobal(new ExhibitContextFactory());
  }

  private final String src;
  private final boolean hasReturn;

  private Context ctx;
  private Scriptable scope;
  private Script script;
  private Function func;

  public JSCalculator(String src) throws ScriptException {
    this.src = src;
    this.hasReturn = src.contains("return");
  }

  @Override
  public void initialize(ExhibitDescriptor descriptor) {
    ctx = Context.enter();
    ctx.setClassShutter(new ClassShutter() {
      @Override
      public boolean visibleToScripts(String className) {
        return false;
      }
    });
    this.scope = ctx.initStandardObjects(null, true); // sealed
    if (hasReturn) {
      this.func = ctx.compileFunction(scope, "function() {" + src + "}", "<cmd>", 1, null);
    } else {
      this.script = ctx.compileString(src, "<cmd>", 1, null);
    }
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

    Object res;
    if (hasReturn) {
      res = func.call(ctx, exhibitScope, null, new Object[0]);
    } else {
      res = script.exec(ctx, exhibitScope);
    }
    System.out.println(res);
    if (res != null) {
      System.out.println(res.getClass());
    }
    return Obs.EMPTY;
  }

  @Override
  public void cleanup() {
    Context.exit();
  }
}
