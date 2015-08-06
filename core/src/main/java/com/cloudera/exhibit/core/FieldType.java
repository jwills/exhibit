package com.cloudera.exhibit.core;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Created by prungta on 8/4/15.
 */
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
