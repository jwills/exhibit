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
package com.cloudera.exhibit.octave;

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import dk.ange.octave.OctaveEngine;
import dk.ange.octave.OctaveEngineFactory;
import dk.ange.octave.OctaveUtils;
import dk.ange.octave.type.OctaveObject;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class OctaveFunctor implements Functor, Serializable{
  private ExhibitDescriptor descriptor;
  private String script;

  private transient OctaveEngine octaveEngine;

  OctaveFunctor(String script) {
    this(null, script);
  }

  OctaveFunctor(ExhibitDescriptor descriptor, String script) {
    this.descriptor = descriptor;
    this.script = script;
    this.octaveEngine = null;
  }

  @Override
  public ExhibitDescriptor initialize(ExhibitDescriptor ed) {
    if(octaveEngine == null) {
      this.octaveEngine = new OctaveEngineFactory().getScriptEngine();
    }
    if(this.descriptor == null){
      Exhibit e = execute(Exhibits.defaultValues2(ed));
      this.descriptor = e.descriptor();
    }
    return this.descriptor;
  }

  @Override
  public void cleanup() {
    if( octaveEngine != null) {
      octaveEngine.close();
      octaveEngine = null;
    }
  }

  @Override
  public Exhibit apply(Exhibit input) {
    this.octaveEngine.eval("clear");
    return execute(input);
  }

  private Map<String, OctaveObject> createOctaveEnv(Exhibit exhibit) {
    Map<String,OctaveObject> vars = Maps.newHashMap();
    Obs attr = exhibit.attributes();
    for (int i = 0; i < attr.descriptor().size(); i++) {
      ObsDescriptor.Field f = attr.descriptor().get(i);
      if( OctaveTypeUtils.isSupportedType(f.type)) {
        Object value = attr.get(i);
        vars.put(f.name, OctaveTypeUtils.convertToOctaveObject(f.type, attr.get(i)));
      }
    }

    for (Map.Entry<String, Frame> f : exhibit.frames().entrySet()) {
      ObsDescriptor od = f.getValue().descriptor();
      if(OctaveTypeUtils.isSupportedType(od)) {
        FieldType ft = od.get(0).type;
        vars.put(f.getKey(), OctaveTypeUtils.convertToOctaveObject(ft, f.getValue()));
      }
    }

    for (Map.Entry<String, Vector> e : exhibit.vectors().entrySet()) {
      String name = e.getKey();
      FieldType type = e.getValue().getType();
      if( OctaveTypeUtils.isSupportedType(type)) {
        vars.put(name, OctaveTypeUtils.convertToOctaveObject(type, e.getValue()));
      }
    }
    return vars;
  }

  private Exhibit execute(Exhibit exhibit) {
    Set<String> originalVars = Sets.newHashSet(OctaveUtils.listVars(octaveEngine).iterator());
    Map<String,OctaveObject> vars = createOctaveEnv(exhibit);
    originalVars.addAll(vars.keySet());
    octaveEngine.putAll(vars);
    octaveEngine.eval(this.script);
    Set<String> newVars = Sets.newHashSet(OctaveUtils.listVars(octaveEngine).iterator());
    Set<String> deltaVars = Sets.difference(newVars,originalVars);
    if(deltaVars.size() <= 0) {
      throw new UnsupportedOperationException("No new variables detected");
    }
    return new OctaveTypeUtils.OctaveConverter(octaveEngine)
        .addVars(deltaVars)
        .convert();
  }
}
