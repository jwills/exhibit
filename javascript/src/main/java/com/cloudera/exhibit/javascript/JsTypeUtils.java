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
import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.Vec;
import com.cloudera.exhibit.core.composite.CompositeExhibit;
import com.cloudera.exhibit.core.simple.SimpleExhibit;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.core.simple.SimpleObs;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.cloudera.exhibit.core.vector.Vector;
import com.cloudera.exhibit.core.vector.VectorBuilder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsTypeUtils {

  private static boolean isCastableToExhibit(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof Map)) {
      return false;
    }
    Map m = (Map) o;
    return Iterables.all(m.entrySet(), new Predicate() {
      @Override
      public boolean apply(Object input) {
        Map.Entry o = (Map.Entry) input;
        return (o.getKey() != null)
            && (o.getKey() instanceof String)
            && (isCastableToField(o.getValue())
            || isCastableToVec(o.getValue())
            || isCastableToFrame(o.getValue()));
      }
    });
  }

  private static boolean isCastableToVec(Object o) {
    if (o == null) {
      return false;
    }
    if (o instanceof ScriptableVec) {
      return true;
    }
    if (!(o instanceof List)) {
      return false;
    }
    List l = (List) o;
    if (l.size() == 0) {
      return true; // TODO: right choice that 0-element list is a vector?
    }
    boolean allCastable = Iterables.all(l, new Predicate() {
      @Override
      public boolean apply(Object input) {
        return isCastableToField(input);
      }
    });
    if (!allCastable) {
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
    if (o == null) {
      return false;
    }
    if (o instanceof ScriptableFrame){
      return true;
    }
    if (!(o instanceof List)) {
      return false;
    }
    List l = (List) o;
    if (l.size() == 0) {
      return false; // made the choice that 0-element is a vector
    }
    boolean allCastable = Iterables.all(l, new Predicate() {
      @Override
      public boolean apply(Object input) {
        return isCastableToObs(input);
      }
    });
    if (!allCastable) {
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
    if (o == null) {
      return false;
    }
    if (!(o instanceof Map)) {
      return false;
    }
    Map m = (Map) o;
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
    return (o instanceof Number) || (o instanceof String) || (o instanceof Boolean);
  }

  private static ObsDescriptor toObsDescriptor(Object res) {
    Map<String, Object> mres = (Map<String, Object>) res;
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
    if (res instanceof ScriptableFrame) {
      ScriptableFrame sf = (ScriptableFrame)res;
      return sf.frame().descriptor();
    }
    if (!(res instanceof List)) {
      throw new IllegalStateException("Unknown object being cast to Frame: " + res);
    }
    List lres = (List) res;
    return toObsDescriptor(lres.get(0));
  }

  static ExhibitDescriptor toExhibitDescriptor(Scriptable res, Set<String> existing) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    List<ObsDescriptor.Field> fields = Lists.newArrayList();
    Map<String, FieldType> vecs = Maps.newHashMap();
    Map<String, ObsDescriptor> frames = Maps.newHashMap();
    for (Object id : res.getIds()) {
      if (!existing.contains(id) && !"INIT".equals(id)) {
        Object v = res.get(id.toString(), res);
        if (isCastableToFrame(v)) {
          frames.put(id.toString(), toFrameDescriptor(v));
        } else if (isCastableToVec(v)) {
          vecs.put(id.toString(), toVectorType(v));
        } else if (isCastableToField(v)) {
          fields.add(new ObsDescriptor.Field(id.toString(), toFieldType(v)));
        }
      }
    }
    return new ExhibitDescriptor(new SimpleObsDescriptor(fields), frames, vecs);
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
    if (o instanceof ScriptableVec) {
      ScriptableVec vec = (ScriptableVec)o;
      return vec.vec().getType();
    }
    if (!(o instanceof List)) {
      throw new IllegalStateException("Unknown object being cast to Vector: " + o);
    }
    List l = (List)o;
    if (l.size() == 0) {
      return FieldType.DOUBLE;
    }
    Object element = l.get(0);
    return toFieldType(element);
  }

  public static Exhibit toExhibit(Scriptable res, ExhibitDescriptor expected) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    List<Object> attrs = Lists.newArrayList();
    Map<String, Frame> frames = Maps.newHashMap();
    Map<String, Vec> vectors = Maps.newHashMap();
    for (ObsDescriptor.Field f : expected.attributes()) {
      attrs.add(toField(res.get(f.name, res)));
    }
    for (Map.Entry<String, FieldType> e : expected.vectors().entrySet()) {
      vectors.put(e.getKey(), toVector(e.getValue(), res.get(e.getKey(), res)));
    }
    for (Map.Entry<String, ObsDescriptor> e : expected.frames().entrySet()) {
      frames.put(e.getKey(), toFrame(e.getValue(), res.get(e.getKey(), res)));
    }
    return new SimpleExhibit(new SimpleObs(expected.attributes(), attrs), frames, vectors);
  }

  private static Exhibit toExhibit(Object res, String missingFieldName) {
    if (res == null) {
      throw new IllegalStateException("Null return values are not permitted");
    }
    if (isCastableToField(res)) {
      ObsDescriptor od = SimpleObsDescriptor.of(missingFieldName, toFieldType(res));
      return new SimpleExhibit(SimpleObs.of(od, toField(res)),
          ImmutableMap.<String, Frame>of(), ImmutableMap.<String, Vec>of());
    } else if (isCastableToVec(res)) {
      return SimpleExhibit.of(missingFieldName, toVector(toVectorType(res), res));
    } else if (isCastableToFrame(res)) {
      return SimpleExhibit.of(missingFieldName, toFrame(toFrameDescriptor(res), res));
    }
    throw new IllegalStateException("Unsupported result type: " + res);
  }

  private static Object toField(Object res) {
    if (res instanceof Number) {
      return ((Number) res).doubleValue();
    } else if (res instanceof String) {
      return res;
    } else if (res instanceof Boolean) {
      return res;
    }
    throw new IllegalStateException("Unsupported result type: " + res);
  }

  private static Frame toFrame(final ObsDescriptor od, Object input) {
    if (input instanceof ScriptableFrame) {
      ScriptableFrame sf = (ScriptableFrame) input;
      return sf.frame();
    }
    if (!(input instanceof List)) {
      throw new IllegalStateException("Unknown object being cast to Frame: " + input);
    }
    List<Object> lres = (List<Object>) input;
    return new SimpleFrame(Lists.transform(lres, new Function<Object, Obs>() {
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
    if (res instanceof ScriptableVec) {
      ScriptableVec vec = (ScriptableVec) res;
      if (!(vec.vec() instanceof Vector)) {
        throw new IllegalStateException("Unknown object being cast to Vector: " + res);
      }
      return (Vector) vec.vec();
    }
    if (!(res instanceof List)) {
      throw new IllegalStateException("Unknown object being cast to Vector: " + res);
    }
    return VectorBuilder.build(fieldType, (List) res);
  }
}