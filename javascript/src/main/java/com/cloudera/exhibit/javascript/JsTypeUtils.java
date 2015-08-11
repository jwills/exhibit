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
import com.cloudera.exhibit.core.composite.CompositeExhibit;
import com.cloudera.exhibit.core.composite.CompositeExhibitDescriptor;
import com.cloudera.exhibit.core.simple.*;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JsTypeUtils {

  private static boolean isCastableToExhibit(Object o) {
    if( o == null ) {
      return false;
    }
    if(!(o instanceof Map)){
      return false;
    }
    Map m = (Map)o;
    return Iterables.all(m.entrySet(), new Predicate() {
      @Override
      public boolean apply(Object input) {
        Map.Entry o = ((Map.Entry) input);
        return (o.getKey() != null)
            && (o.getKey() instanceof String)
            && (isCastableToField(o.getValue())
            || isCastableToVec(o.getValue())
            || isCastableToFrame(o.getValue()));
      }
    });
  }
  private static boolean isCastableToVec(Object o){
    if( o == null ) {
      return false;
    }
    if (o instanceof ScriptableVec) {
      return true;
    }
    if(!(o instanceof List)){
      return false;
    }
    List l = (List)o;
    if(l.size() == 0){
      return true; // made the choice that 0-element list is a vector
    }
    boolean allCastable = Iterables.all(l, new Predicate() {
      @Override
      public boolean apply(Object input) {
        return isCastableToField(input);
      }
    });
    if(!allCastable){
      return false;
    }
    Object firstElement = l.get(0);
    final FieldType ft = toFieldType(firstElement);
    return Iterables.all(l, new Predicate() {
      @Override
      public boolean apply(Object input) {
        return ft.equals(toFieldType(input));
      }
    });
  }

  private static boolean isCastableToFrame(Object o){
    if( o == null ) {
      return false;
    }
    if(o instanceof ScriptableFrame){
      return true;
    }
    if(!(o instanceof List)){
      return false;
    }
    List l = (List)o;
    if(l.size() == 0){
      return false; // made the choice that 0-element is a vector
    }
    boolean allCastable = Iterables.all(l, new Predicate() {
      @Override
      public boolean apply(Object input) {
        return isCastableToObs(input);
      }
    });
    if(!allCastable){
      return false;
    }
    Object firstElement = l.get(0);
    final ObsDescriptor od = toObsDescriptor(firstElement);
    return Iterables.all(l, new Predicate() {
      @Override
      public boolean apply(Object input) {
        return od.equals(toObsDescriptor(input));
      }
    });
  }

  private static boolean isCastableToObs(Object o) {
    if( o == null ) {
      return false;
    }
    if(!(o instanceof Map)){
      return false;
    }
    Map m = (Map)o;
    return Iterables.all(m.entrySet(), new Predicate() {
      @Override
      public boolean apply(Object input) {
        Map.Entry o = ((Map.Entry) input);
        return (o.getKey() != null)
            && (o.getKey() instanceof String)
            && isCastableToField(o.getValue());
      }
    });
  }

  private static boolean isCastableToField(Object o) {
    return (o instanceof Number)
        || (o instanceof String)
        || (o instanceof Boolean);
  }

  private static ObsDescriptor toObsDescriptor(Object res) {
    Map<String, Object> mres = (Map<String, Object>)res;
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    for (String key : Sets.newTreeSet(mres.keySet())) {
      Object val = mres.get(key);
      FieldType ft = null;
      if (val == null) {
        throw new IllegalStateException("Null value for key: " + key);
      } else if (val instanceof Number) {
        ft = FieldType.DOUBLE;
      } else if (val instanceof String) {
        ft = FieldType.STRING;
      } else if (val instanceof Boolean) {
        ft = FieldType.BOOLEAN;
      }
      fields.add(new ObsDescriptor.Field(key, ft));
    }
    return new SimpleObsDescriptor(fields);
  }

  private static ObsDescriptor toFrameDescriptor(Object res) {
    if(res instanceof ScriptableFrame){
      ScriptableFrame sf = (ScriptableFrame)res;
      return sf.getFrame().descriptor();
    }
    if(!(res instanceof List)){
      throw new IllegalStateException("Unknown object being cast to Frame: " + res);
    }
    List lres = (List) res;
    return toObsDescriptor(lres.get(0));
  }

  protected static ExhibitDescriptor toExhibitDescriptor(Object res) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    if (isCastableToExhibit(res)) {
      Iterable<ExhibitDescriptor> eds = (Iterable<ExhibitDescriptor>) Maps.transformEntries((Map<String, Object>)res,
          new Maps.EntryTransformer<String, Object, ExhibitDescriptor>() {
            @Override
            public ExhibitDescriptor transformEntry(String fieldName, Object fieldValue) {
              return toExhibitDescriptor(fieldValue, fieldName);
            }
          }).values();
      return new CompositeExhibitDescriptor(eds);
    }
    return toExhibitDescriptor(res, JsFunctorConstants.DEFAULT_FIELD_NAME);
  }

  private static ExhibitDescriptor toExhibitDescriptor(Object res, String missingFieldName) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    if (isCastableToField(res)) {
      return new SimpleExhibitDescriptor(SimpleObsDescriptor.of(missingFieldName, toFieldType(res)),
          Collections.<String, ObsDescriptor>emptyMap(), Collections.<String, FieldType>emptyMap());
    } else if (isCastableToVec(res)) {
      return new SimpleExhibitDescriptor(ObsDescriptor.EMPTY,
          Collections.<String, ObsDescriptor>emptyMap(), ImmutableMap.of(missingFieldName, toVectorType(res)));
    } else if (isCastableToFrame(res)) {
      return new SimpleExhibitDescriptor(ObsDescriptor.EMPTY,
        ImmutableMap.of(missingFieldName, toFrameDescriptor(res)), Collections.<String,FieldType>emptyMap());
    }
    throw new IllegalStateException("Unsupported result type: " + res);
  }

  private static FieldType toFieldType(Object res) {
    if (res instanceof Number) {
      return FieldType.DOUBLE;
    } else if (res instanceof String) {
      return FieldType.STRING;
    } else if (res instanceof Boolean) {
      return FieldType.BOOLEAN;
    }
    throw new IllegalStateException("Unsupported result type: " + res);
  }

  private static FieldType toVectorType(Object o) {
    if(o instanceof  ScriptableVec){
      ScriptableVec vec = (ScriptableVec)o;
      return vec.getVec().getType();
    }
    if(!(o instanceof List)){
      throw new IllegalStateException("Unknown object being cast to Vector: " + o);
    }
    List l = (List)o;
    if(l.size() == 0){
      return FieldType.DOUBLE;
    }
    Object element = l.get(0);
    return toFieldType(element);
  }

  public static Exhibit toExhibit(Object res) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    if (isCastableToExhibit(res)) {
      Iterable<Exhibit> eds = (Iterable<Exhibit>) Maps.transformEntries((Map<String, Object>)res,
          new Maps.EntryTransformer<String, Object, Exhibit>() {
            @Override
            public Exhibit transformEntry(String fieldName, Object fieldValue) {
              return toExhibit(fieldValue, fieldName);
            }
          }).values();
      return new CompositeExhibit(eds);
    }
    return toExhibit(res, JsFunctorConstants.DEFAULT_FIELD_NAME);
  }

  private static Exhibit toExhibit(Object res, String missingFieldName) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    if (isCastableToField(res)) {
      ObsDescriptor od = SimpleObsDescriptor.of(missingFieldName, toFieldType(res));
      return new SimpleExhibit(SimpleObs.of(od, toField(res)),
          Collections.<String, Frame>emptyMap(), Collections.<String, Vector>emptyMap());
    } else if (isCastableToVec(res)) {
      return SimpleExhibit.of(missingFieldName, toVector(toVectorType(res), res));
    } else if (isCastableToFrame(res)) {
      return SimpleExhibit.of(missingFieldName, toFrame(toFrameDescriptor(res), res));
    }
   throw new IllegalStateException("Unsupported result type: " + res);
  }

  private static Object toField(Object res) {
    if (res instanceof Number) {
      return ((Number)res).doubleValue();
    } else if (res instanceof String) {
      return res;
    } else if (res instanceof Boolean) {
      return res;
    }
    throw new IllegalStateException("Unsupported result type: " + res);
  }

  private static Frame toFrame(final ObsDescriptor od, Object input) {
    if(input instanceof ScriptableFrame){
      ScriptableFrame sf = (ScriptableFrame)input;
      return sf.getFrame();
    }
    if(!(input instanceof List)){
      throw new IllegalStateException("Unknown object being cast to Frame: " + input);
    }
    List lres = (List)input;
    return SimpleFrame.of(Lists.transform(lres, new Function<Object, Obs>() {
      @Override
      public Obs apply(Object res) {
        List<Object> values = Lists.newArrayListWithExpectedSize(od.size());
        if (res instanceof Map) {
          Map mres = (Map) res;
          for (ObsDescriptor.Field f : od) {
            Object v = mres.get(f.name);
            values.add(v == null ? null : f.type.cast(v));
          }
        } else if (od.size() == 1) {
          if (res == null) {
            values.add(null);
          } else {
            values.add(od.get(0).type.cast(res));
          }
        } else {
          throw new IllegalStateException("Invalid javascript result: " + res);
        }
        return new SimpleObs(od, values);
      }
    }));
  }

  private static Vector toVector(FieldType fieldType, Object res) {
    if(res instanceof  ScriptableVec){
      ScriptableVec vec = (ScriptableVec)res;
      if(!(vec.getVec() instanceof Vector)) {
        throw new IllegalStateException("Unknown object being cast to Vector: " + res);
      }
      Vector vector = (Vector)vec.getVec();
      return vector;
    }
    if(!(res instanceof List)){
      throw new IllegalStateException("Unknown object being cast to Vector: " + res);
    }
    return VectorBuilder.build(fieldType, (List)res);
  }
}
