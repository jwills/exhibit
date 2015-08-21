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
package com.cloudera.exhibit.core;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public enum FieldType {
  BOOLEAN {
    public Object cast(Object in) {
      if (in instanceof Boolean) {
        return in;
      } else if (in instanceof Number) {
        return ((Number) in).doubleValue() != 0.0;
      } else if (in instanceof String) {
        return !("false".equalsIgnoreCase(in.toString()));
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to boolean value");
      }
    }
  },
  SHORT {
    public Object cast(Object in) {
      if (in instanceof Number) {
        return ((Number) in).shortValue();
      } else if (in instanceof String) {
        return Short.valueOf(((String) in));
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to short value");
      }
    }
  },
  INTEGER {
    public Object cast(Object in) {
      if (in instanceof Number) {
        return ((Number) in).intValue();
      } else if (in instanceof String) {
        return Integer.valueOf(((String) in));
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to int value");
      }
    }
  },
  LONG {
    public Object cast(Object in) {
      if (in instanceof Number) {
        return ((Number) in).longValue();
      } else if (in instanceof String) {
        return Long.valueOf((String) in);
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to long value");
      }
    }
  },
  FLOAT {
    public Object cast(Object in) {
      if (in instanceof Number) {
        return ((Number) in).floatValue();
      } else if (in instanceof String) {
        return Float.valueOf((String) in);
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to float value");
      }
    }
  },
  DOUBLE {
    public Object cast(Object in) {
      if (in instanceof Number) {
        return ((Number) in).doubleValue();
      } else if (in instanceof String) {
        return Double.valueOf((String) in);
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to double value");
      }
    }
  },
  STRING {
    public Object cast(Object in) {
      return in.toString();
    }
  },
  DATE {
    public Object cast(Object in) {
      if (in instanceof Date) {
        return in;
      } else if (in instanceof Number) {
        return new Date(((Number) in).longValue());
      } else if (in instanceof String) {
        return Date.valueOf(in.toString());
      }
      throw new IllegalArgumentException("Cannot cast " + in + " to date value");
    }
  },
  TIME {
    @Override
    public Object cast(Object in) {
      if (in instanceof Time) {
        return in;
      } else if (in instanceof Number) {
        return new Time(((Number) in).longValue());
      } else if (in instanceof String) {
        return Time.valueOf(in.toString());
      }
      throw new IllegalArgumentException("Cannot cast " + in + " to time value");
    }
  },
  TIMESTAMP {
    public Object cast(Object in) {
      if (in instanceof Timestamp) {
        return in;
      } else if (in instanceof Number) {
        return new Timestamp(((Number) in).longValue());
      } else if (in instanceof String) {
        return Timestamp.valueOf(in.toString());
      }
      throw new IllegalArgumentException("Cannot cast " + in + " to timestamp value");
    }
  },
  DECIMAL {
    public Object cast(Object in) {
      if (in instanceof BigDecimal) {
        return in;
      } else if (in instanceof Number) {
        return new BigDecimal(((Number) in).doubleValue());
      } else if (in instanceof String) {
        return new BigDecimal(in.toString());
      } else {
        throw new IllegalArgumentException("Cannot cast " + in + " to decimal value");
      }
    }
  };

  public abstract Object cast(Object in);
}
