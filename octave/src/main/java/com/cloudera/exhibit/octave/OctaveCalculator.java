package com.cloudera.exhibit.octave;

import com.cloudera.exhibit.core.*;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import dk.ange.octave.*;
import dk.ange.octave.type.OctaveObject;

/**
 * Created by prungta on 8/5/15.
 */
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
    if(octaveEngine == null) {
      this.octaveEngine = new OctaveEngineFactory().getScriptEngine();
    }
    if(this.descriptor == null){
      OctaveFrame of = execute(Exhibits.defaultValues(ed));
      this.descriptor = of.descriptor();
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
  public Iterable<Obs> apply(Exhibit input) {
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

  private OctaveFrame execute(Exhibit exhibit) {
    Set<String> originalVars = Sets.newHashSet(OctaveUtils.listVars(octaveEngine).iterator());
    Map<String,OctaveObject> vars = createOctaveEnv(exhibit);
    originalVars.addAll(vars.keySet());
    octaveEngine.putAll(vars);
    octaveEngine.eval(this.script);
    Set<String> newVars = Sets.newHashSet(OctaveUtils.listVars(octaveEngine).iterator());
    Set<String> deltaVars = Sets.difference(newVars,originalVars);
    if(deltaVars.size() != 1) {
      throw new UnsupportedOperationException("Currently only support the extraction of a single variable");
    }
    String varName = Iterables.getOnlyElement(deltaVars);
    return new OctaveFrame(varName, octaveEngine.get(varName));
  }

}
