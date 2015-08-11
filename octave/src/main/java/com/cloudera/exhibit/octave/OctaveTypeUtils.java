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

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.core.vector.BooleanVector;
import com.cloudera.exhibit.core.vector.DoubleVector;
import com.cloudera.exhibit.core.vector.IntVector;
import com.cloudera.exhibit.core.vector.Vector;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dk.ange.octave.OctaveEngine;
import dk.ange.octave.type.OctaveBoolean;
import dk.ange.octave.type.OctaveDouble;
import dk.ange.octave.type.OctaveInt;
import dk.ange.octave.type.OctaveObject;
import dk.ange.octave.type.matrix.AbstractGenericMatrix;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OctaveTypeUtils {

  public static class OctaveConverter{
    OctaveEngine octaveEngine;
    List<Object> attrValues;
    List<ObsDescriptor.Field> attrDesc;
    Map<String, Frame> frames;
    Map<String, Vector> vectors;

    public OctaveConverter(OctaveEngine octaveEngine) {
      this.octaveEngine = octaveEngine;
      this.attrValues = Lists.newArrayListWithExpectedSize(10);
      this.attrDesc = Lists.newArrayListWithExpectedSize(10);
      this.frames = Maps.newHashMap();
      this.vectors = Maps.newHashMap();
    }

    private OctaveConverter addVar(String var){
      OctaveObject octaveObject = octaveEngine.get(var);
      if(!(octaveObject instanceof AbstractGenericMatrix)){
        throw new IllegalArgumentException("Unsupported Type: " +octaveObject.toString());
      }
      AbstractGenericMatrix mat = (AbstractGenericMatrix) octaveObject;
      int [] dims = mat.getSize();
      if( dims.length < 1 || dims.length > 2) {
        throw new IllegalArgumentException("Unsupported dimensions: " + Arrays.asList(dims).toString());
      }
      if(dims[0]==1 && (dims.length == 1 || (dims.length == 2 && dims[1] == 1))){
        return addAttribute(var, octaveObject);
      } else if(dims.length == 1 || (dims.length == 2 && dims[1] == 1)){
        return addVector(var, octaveObject);
      } else if(dims.length == 2) {
        return addFrame(var, octaveObject);
      }
      return this;
    }

    private OctaveConverter addVector(String var, OctaveObject octaveObject) {
      if(octaveObject instanceof OctaveDouble){
        OctaveDouble od = (OctaveDouble)octaveObject;
        vectors.put(var, new DoubleVector(od.getData()));
      } else if(octaveObject instanceof OctaveInt){
        OctaveInt oi = (OctaveInt)octaveObject;
        vectors.put(var, new IntVector(oi.getData()));
      } else if(octaveObject instanceof OctaveBoolean){
        OctaveBoolean ob = (OctaveBoolean)octaveObject;
        vectors.put(var, new BooleanVector(ob.getData()));
      } else {
        throw new IllegalArgumentException("Unsupported Type: " + octaveObject.toString());
      }
      return this;
    }

    private OctaveConverter addFrame(String var, OctaveObject octaveObject) {
      frames.put(var, new OctaveFrame(var, octaveObject));
      return this;
    }

    private OctaveConverter addAttribute(String name, FieldType type, Object value) {
      attrDesc.add(new ObsDescriptor.Field(name, type));
      attrValues.add(value);
      return this;
    }

    private OctaveConverter addAttribute(String var, OctaveObject octaveObject) {
      if(octaveObject instanceof OctaveDouble){
        OctaveDouble od = (OctaveDouble)octaveObject;
        return addAttribute(var, FieldType.DOUBLE, od.get(1,1));
      } else if(octaveObject instanceof OctaveInt){
        OctaveInt oi = (OctaveInt)octaveObject;
        return addAttribute(var, FieldType.INTEGER, oi.get(1,1));
      } else if(octaveObject instanceof OctaveBoolean){
        OctaveBoolean oi = (OctaveBoolean)octaveObject;
        return addAttribute(var, FieldType.BOOLEAN, oi.get(1,1));
      }
      throw new IllegalArgumentException("Unsupported Type: " +octaveObject.toString());
    }

    public OctaveConverter addVars(Iterable<String> vars){
      for(String var: vars){
        addVar(var);
      }
      return this;
    }

    public Exhibit convert(){
      ObsDescriptor od = new SimpleObsDescriptor(attrDesc);
      return new SimpleExhibit(new SimpleObs(od, attrValues), frames, vectors);
    }
  }


  public static boolean isSupportedType(ObsDescriptor od){
    if(od.size() == 0){
      return false;
    }
    FieldType ft = od.get(0).type;
    Iterator<ObsDescriptor.Field> it = od.iterator();
    while(it.hasNext()) {
      if( it.next().type != ft ) {
        return false;
      }
    }
    return isSupportedType(ft);
  }

  public static boolean isSupportedType(FieldType f){
    switch (f)
    {
      case BOOLEAN:
      case INTEGER:
      case SHORT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return true;
      case STRING:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case DECIMAL:
        return false;
    }
    return false;
  }

  public static OctaveObject convertToOctaveObject(FieldType originalType, Object o) {
    switch(originalType){
      case BOOLEAN:
        return new OctaveBoolean(new boolean[]{(Boolean)o},1,1);
      case SHORT:
      case INTEGER:
        return new OctaveInt(new int[]{(Integer)o},1,1);
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new OctaveDouble(new double[]{(Double)o},1,1);
    }
    throw new UnsupportedOperationException("Unsupported Type: " +originalType.toString());
  }

  public static OctaveObject convertToOctaveObject(FieldType originalType, Vector v) {
    switch(originalType){
      case BOOLEAN:
        return new OctaveBoolean(((BooleanVector)v).getData(),v.size(),1);
      case SHORT:
      case INTEGER:
        return new OctaveInt(((IntVector)v).getData(),v.size(),1);
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new OctaveDouble(((DoubleVector)v).getData(),v.size(),1);
    }
    throw new UnsupportedOperationException("Unsupported Type: " +originalType.toString());
  }

  public static OctaveObject convertToOctaveObject(FieldType originalType, Frame f) {
    ObsDescriptor od = f.descriptor();
    switch(originalType){
      case BOOLEAN:
        return new OctaveBoolean(convertFrameToBool(f), f.size(), od.size());
      case SHORT:
      case INTEGER:
        return new OctaveInt(convertFrameToInt(f), f.size(), od.size());
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new OctaveDouble(convertFrameToDouble(f), f.size(), od.size());
    }
    throw new UnsupportedOperationException("Unsupported Type: " +originalType.toString());
  }

  private static int[] convertFrameToInt(Frame f) {
    int size = f.size() * f.descriptor().size();
    int [] b = new int[size];
    for (int j = 0; j < f.descriptor().size(); j++) {
      for (int i = 0; i < f.size(); i++) {
        b[j*f.size() + i] = (Integer)f.get(i).get(j);
      }
    }
    return b;
  }

  private static double[] convertFrameToDouble(Frame f) {
    int size = f.size() * f.descriptor().size();
    double [] b = new double[size];
    for (int j = 0; j < f.descriptor().size(); j++) {
      for (int i = 0; i < f.size(); i++) {
        b[j*f.size() + i] = (Double)f.get(i).get(j);
      }
    }
    return b;
  }

  private static boolean[] convertFrameToBool(Frame f) {
    int size = f.size() * f.descriptor().size();
    boolean [] b = new boolean[size];
    for (int j = 0; j < f.descriptor().size(); j++) {
      for (int i = 0; i < f.size(); i++) {
        b[j*f.size() + i] = (Boolean)f.get(i).get(j);
      }
    }
    return b;
  }
}
