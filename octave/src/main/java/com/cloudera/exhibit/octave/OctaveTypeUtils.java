package com.cloudera.exhibit.octave;

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.vector.BooleanVector;
import com.cloudera.exhibit.core.vector.DoubleVector;
import com.cloudera.exhibit.core.vector.IntVector;
import com.cloudera.exhibit.core.vector.Vector;
import dk.ange.octave.type.OctaveBoolean;
import dk.ange.octave.type.OctaveDouble;
import dk.ange.octave.type.OctaveInt;
import dk.ange.octave.type.OctaveObject;

import java.util.Iterator;

/**
 * Created by prungta on 8/5/15.
 */
public class OctaveTypeUtils {

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
