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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Exhibits;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import dk.ange.octave.OctaveEngine;
import dk.ange.octave.OctaveEngineFactory;
import dk.ange.octave.OctaveUtils;
import dk.ange.octave.type.OctaveObject;

public class OctaveCalculator implements Calculator, Serializable{
  private ObsDescriptor descriptor;
  private String script;

  private transient OctaveEngine octaveEngine;

  OctaveCalculator(String script) throws OctaveScriptFormatException {
    this(null, script);
  }

  OctaveCalculator(ObsDescriptor descriptor, String script) throws OctaveScriptFormatException {
    this.descriptor = descriptor;
    this.script = script;
    this.octaveEngine = null;
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor ed) {
    if (octaveEngine == null) {
      this.octaveEngine = new OctaveEngineFactory().getScriptEngine();
    }
    if (this.descriptor == null) {
      Iterable<Obs> of = execute(Exhibits.defaultValues(ed));
      if (of instanceof Frame) {
        this.descriptor = ((Frame) of).descriptor();
      } else {
        this.descriptor = Iterables.get(of, 0).descriptor();
      }
    }
    return this.descriptor;
  }

  @Override
  public void cleanup() {
    if (octaveEngine != null) {
      octaveEngine.close();
      octaveEngine = null;
    }
  }

  @Override
  public Iterable<Obs> apply(Exhibit input) {
    this.octaveEngine.eval("clear");
    return execute(input);
  }

  private Map<String, OctaveObject> createOctaveEnv(Exhibit exhibit) {
    Map<String, OctaveObject> vars = Maps.newHashMap();
    Obs attr = exhibit.attributes();
    for (int i = 0; i < attr.descriptor().size(); i++) {
      ObsDescriptor.Field f = attr.descriptor().get(i);
      if( OctaveTypeUtils.isSupportedType(f.type)) {
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

    for (Map.Entry<String, Vec> e : exhibit.vectors().entrySet()) {
      String name = e.getKey();
      FieldType type = e.getValue().getType();
      if (OctaveTypeUtils.isSupportedType(type)) {
        vars.put(name, OctaveTypeUtils.convertToOctaveObject(type, e.getValue()));
      }
    }
    return vars;
  }

  private Iterable<Obs> execute(Exhibit exhibit) {
    Set<String> originalVars = Sets.newHashSet(OctaveUtils.listVars(octaveEngine).iterator());
    Map<String, OctaveObject> vars = createOctaveEnv(exhibit);
    originalVars.addAll(vars.keySet());
    octaveEngine.putAll(vars);
    octaveEngine.eval(this.script);
    Set<String> newVars = Sets.newHashSet(OctaveUtils.listVars(octaveEngine).iterator());
    Set<String> deltaVars = Sets.difference(newVars,originalVars);
    if(deltaVars.size() != 1) {
      throw new UnsupportedOperationException("Currently only support the extraction of a single variable");
    }
    String varName = Iterables.getOnlyElement(deltaVars);
    Exhibit result = new OctaveTypeUtils.OctaveConverter(octaveEngine)
        .addVars(deltaVars)
        .convert();
    if (result.frames().containsKey(varName)) {
      return result.frames().get(varName);
    } else if (result.vectors().containsKey(varName)) {
      return result.vectors().get(varName).asFrame();
    } else {
      return ImmutableList.of(result.attributes());
    }
  }

}
