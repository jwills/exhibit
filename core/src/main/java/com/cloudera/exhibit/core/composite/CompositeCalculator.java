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
package com.cloudera.exhibit.core.composite;

import com.cloudera.exhibit.core.Calculator;
import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CompositeCalculator implements Calculator {

  private List<Calculator> calculators;

  public static CompositeCalculator of(Calculator... calcs) {
    return new CompositeCalculator(Arrays.asList(calcs));
  }

  public CompositeCalculator(List<Calculator> calculators) {
    this.calculators = calculators;
  }

  @Override
  public ObsDescriptor initialize(ExhibitDescriptor descriptor) {
    List<ObsDescriptor> ret = Lists.newArrayList();
    for (Calculator c : calculators) {
      ret.add(c.initialize(descriptor));
    }
    return new CompositeObsDescriptor(ret);
  }

  @Override
  public void cleanup() {
    for (Calculator c : calculators) {
      c.cleanup();
    }
  }

  @Override
  public Iterable<Obs> apply(Exhibit exhibit) {
    List<Iterable<Obs>> rets = Lists.newArrayList();
    for (Calculator c : calculators) {
      rets.add(c.apply(exhibit));
    }
    return new ChainObsIterable(rets);
  }

  private static class ChainObsIterable implements Iterable<Obs> {

    private List<Iterable<Obs>> items;

    public ChainObsIterable(List<Iterable<Obs>> items) {
      this.items = items;
    }

    @Override
    public Iterator<Obs> iterator() {
      final List<Iterator<Obs>> iters = Lists.transform(items, new Function<Iterable<Obs>, Iterator<Obs>>() {
        @Override
        public Iterator<Obs> apply(Iterable<Obs> obs) {
          return obs.iterator();
        }
      });
      final List<Obs> current = Lists.newArrayList();
      for (int i = 0; i < iters.size(); i++) {
        current.add(null);
      }
      return new Iterator<Obs>() {

        @Override
        public boolean hasNext() {
          for (Iterator<Obs> it : iters) {
            if (it.hasNext()) {
              return true;
            }
          }
          return false;
        }

        @Override
        public Obs next() {
          boolean hasNext = false;
          for (int i = 0; i < iters.size(); i++) {
            if (iters.get(i).hasNext()) {
              current.set(i, iters.get(i).next());
              hasNext = true;
            } else if (current.get(i) == null) {
              throw new IllegalStateException("Invalid obs for position: " + i);
            }
          }
          if (!hasNext) {
            throw new IllegalStateException("Empty composite iterator");
          }
          return new CompositeObs(current);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
