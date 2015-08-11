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

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.vector.Vector;
import org.mozilla.javascript.*;

import java.io.Serializable;
import java.util.Map;

public class JSFunctor implements Serializable, Functor {

  private String src;
  private boolean hasReturn;

  private transient ExhibitDescriptor descriptor;
  private transient Context ctx = null;
  private transient Scriptable scope;
  private transient Script script;
  private transient Function func;

  public JSFunctor(String src) {
    this(null, src);
  }

  public JSFunctor(ExhibitDescriptor descriptor, String src) {
    this.src = src;
    this.hasReturn = src.contains("return");
    this.descriptor = descriptor;
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor ed) {
    if( !ContextFactory.hasExplicitGlobal() ){
      ContextFactory.initGlobal(new ExhibitContextFactory());
    }
    if (ctx == null) {
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
    }

    if (this.descriptor == null) {
      Object returnedObject = eval(Exhibits.defaultValues(ed));
      this.descriptor = JsTypeUtils.toExhibitDescriptor(returnedObject);
    }
    return descriptor;
  }

  @Override
  public Exhibit apply(Exhibit exhibit) {
    return JsTypeUtils.toExhibit(eval(exhibit));
  }

  private Object eval(Exhibit exhibit) {
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
    for (Map.Entry<String, Vector> e : exhibit.vectors().entrySet()) {
      exhibitScope.put(e.getKey(), exhibitScope, new ScriptableVec(e.getValue()));
    }

    if (hasReturn) {
      return func.call(ctx, exhibitScope, null, new Object[0]);
    } else {
      return script.exec(ctx, exhibitScope);
    }
  }

  @Override
  public void cleanup() {
    if( ctx != null ) {
      Context.exit();
      ctx = null;
    }
  }
}
