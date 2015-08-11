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

import com.cloudera.exhibit.core.FieldType;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.cloudera.exhibit.core.simple.SimpleObsDescriptor;
import com.google.common.base.Preconditions;
import dk.ange.octave.type.OctaveBoolean;
import dk.ange.octave.type.OctaveDouble;
import dk.ange.octave.type.OctaveInt;
import dk.ange.octave.type.OctaveObject;

import java.util.Iterator;

public class OctaveFrame extends Frame {
  private OctaveObject octaveObject;
  private ObsDescriptor descriptor;
  private String varName;
  private FieldType ft;

  public OctaveFrame(String varName, OctaveObject octaveObject) {
    this.varName = varName;
    this.octaveObject = octaveObject;
    if(octaveObject instanceof OctaveDouble){
      this.ft = FieldType.DOUBLE;
      OctaveDouble od = (OctaveDouble) octaveObject;
      this.descriptor = buildObsDescriptor(varName, ft, od.getSize());
    } else if(octaveObject instanceof OctaveInt){
      this.ft = FieldType.INTEGER;
      OctaveInt oi = (OctaveInt) octaveObject;
      this.descriptor = buildObsDescriptor(varName, ft, oi.getSize());
    } else if(octaveObject instanceof OctaveBoolean){
      this.ft = FieldType.BOOLEAN;
      OctaveBoolean ob = (OctaveBoolean) octaveObject;
      this.descriptor = buildObsDescriptor(varName, ft, ob.getSize());
    } else {
      throw new IllegalArgumentException("Unsupported Type: " +octaveObject.toString());
    }
  }

  private ObsDescriptor buildObsDescriptor(String baseName, FieldType type, int[] dims) {
    Preconditions.checkArgument(dims.length == 2);
    Preconditions.checkArgument(dims[0] != 1);
    Preconditions.checkArgument(dims[1] != 1);
    SimpleObsDescriptor.Builder builder = new SimpleObsDescriptor.Builder();
    for (int i = 0; i < dims[1]; i++) {
      builder.add(baseName + "$" + i, type);
    }
    return  builder.build();
  }

  @Override
  public ObsDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int size() {
    if( ft == FieldType.BOOLEAN ) {
      return ((OctaveBoolean) octaveObject).size(1);
    } if( ft == FieldType.DOUBLE ) {
      return ((OctaveDouble) octaveObject).size(1);
    } if( ft == FieldType.INTEGER ) {
      return ((OctaveInt) octaveObject).size(1);
    }
    return 0;
  }

  @Override
  public Obs get(int rowIndex) {
    return new OcObs(rowIndex);
  }

  public class OcObs extends Obs {
    private int rowIndex;

    public OcObs(int i) {
      rowIndex = i;
    }

    @Override
    public ObsDescriptor descriptor() {
      return descriptor;
    }

    @Override
    public int size() {
      return descriptor().size();
    }

    @Override
    public Object get(int cIndex) {
      if( ft == FieldType.BOOLEAN ) {
        return ((OctaveBoolean) octaveObject).get(rowIndex+1, cIndex+1);
      } if( ft == FieldType.DOUBLE ) {
        return ((OctaveDouble) octaveObject).get(rowIndex + 1, cIndex + 1);
      } if( ft == FieldType.INTEGER ) {
        return ((OctaveInt) octaveObject).get(rowIndex + 1, cIndex + 1);
      }
      return null;
    }
  }

  @Override
  public Iterator<Obs> iterator() {
    return new OcObsIterator();
  }

  public class OcObsIterator implements Iterator<Obs>{
    int offset = 0;
    @Override
    public boolean hasNext() {
      return offset < size();
    }

    @Override
    public Obs next() {
      offset++;
      return new OcObs(offset - 1);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
