package com.cloudera.exhibit.etl.tbl;

import java.util.Map;

public enum TblType {
  SUM {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new SumTbl(values);
    }
  },
  SUM_TOP {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new SumTopTbl(values, options);
    }
  },
  PERCENTILE {
    @Override
    public Tbl create(Map<String, String> values, Map<String, Object> options) {
      return new PercentileTbl(values, options);
    }
  }
  ;

  public abstract Tbl create(Map<String, String> values, Map<String, Object> options);
}
