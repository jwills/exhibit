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
package com.cloudera.exhibit.etl;

import com.cloudera.exhibit.core.*;
import com.cloudera.exhibit.core.simple.SimpleFrame;
import com.cloudera.exhibit.javascript.JSFunctor;
import com.cloudera.exhibit.sql.SQLFunctor;
import com.google.common.collect.Iterables;

public class MIGRATION_UTILITIES {
  // temporary utilities
  // DO NOT USE THIS CLASS, IT WILL BE REMOVED ASAP

  public static Frame eval(Functor f, Exhibit e){
    Exhibit result = f.apply(e);
    if( f instanceof SQLFunctor){
      // makes the huge assumption that the only frame to be returned
      // is the default-named frame produced by the SQLFunctor
      return result.frames().get(SQLFunctor.DEFAULT_RESULT_FRAME);
    } else if ( f instanceof JSFunctor){
      // earlier implementation of JSCalculator only returned summarized values
      if(result.frames().size()==1){
        return Iterables.getOnlyElement(result.frames().values());
      }
      return SimpleFrame.of(result.attributes());
    }
    throw new UnsupportedOperationException("Unsupported functor: " + f.toString() + " exhibit: " + e.toString());
  }

  public static ObsDescriptor initialize(Functor f, ExhibitDescriptor e){
    ExhibitDescriptor result = f.initialize(e);
    if( f instanceof SQLFunctor){
      // makes the huge assumption that the only frame to be returned
      // is the default-named frame produced by the SQLFunctor
      return result.frames().get(SQLFunctor.DEFAULT_RESULT_FRAME);
    } else if ( f instanceof JSFunctor){
      // earlier implementation of JSCalculator only returned summarized values
      return result.attributes();
    }
    throw new UnsupportedOperationException("Unsupported functor: " + f.toString() + " exhibit: " + e.toString());
  }
}
