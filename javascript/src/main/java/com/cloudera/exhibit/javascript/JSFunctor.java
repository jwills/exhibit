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
package com.cloudera.exhibit.javascript;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Exhibits;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Functor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.Vec;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;

import java.io.Serializable;
import java.util.Map;

public class JSFunctor implements Serializable, Functor {

  private String src;

  private transient ExhibitDescriptor descriptor;
  private transient Context ctx = null;
  private transient Scriptable scope;
  private transient Script script;

  public JSFunctor(String src) {
    this(null, src);
  }

  public JSFunctor(ExhibitDescriptor descriptor, String src) {
    this.src = src;
    this.descriptor = descriptor;
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor ed) {
    if (!ContextFactory.hasExplicitGlobal()) {
      ContextFactory.initGlobal(new ExhibitContextFactory());
    }
    if (ctx == null) {
      ctx = Context.enter();
      this.scope = ctx.initStandardObjects(null, true);
      this.script = ctx.compileString(src, "<cmd>", 1, null);
    }

    if (this.descriptor == null) {
      Exhibit defaultValues = Exhibits.defaultValues(ed);
      Scriptable scope = eval(defaultValues, true);
      this.descriptor = JsTypeUtils.toExhibitDescriptor(scope, Exhibits.nameSet(ed));
    }
    return descriptor;
  }

  @Override
  public Exhibit apply(Exhibit exhibit) {
    return JsTypeUtils.toExhibit(eval(exhibit, false), descriptor);
  }

  private Scriptable eval(Exhibit exhibit, boolean init) {
    Scriptable exhibitScope = ctx.newObject(scope);
    exhibitScope.setPrototype(scope);
    exhibitScope.setParentScope(null);
    exhibitScope.put("INIT", exhibitScope, init);

    Obs attr = exhibit.attributes();
    for (int i = 0; i < attr.descriptor().size(); i++) {
      exhibitScope.put(attr.descriptor().get(i).name, exhibitScope, attr.get(i));
    }
    for (Map.Entry<String, Frame> e : exhibit.frames().entrySet()) {
      exhibitScope.put(e.getKey(), exhibitScope, new ScriptableFrame(e.getValue()));
    }
    for (Map.Entry<String, Vec> e : exhibit.vectors().entrySet()) {
      exhibitScope.put(e.getKey(), exhibitScope, new ScriptableVec(e.getValue()));
    }
    script.exec(ctx, exhibitScope);
    return exhibitScope;
  }

  @Override
  public void cleanup() {
    if (ctx != null) {
      Context.exit();
      ctx = null;
    }
  }
}