
CREATE TEMPORARY FUNCTION within AS 'com.cloudera.exhibit.hive.WithinUDF';
CREATE TEMPORARY FUNCTION within_table AS 'com.cloudera.exhibit.hive.WithinUDTF';
CREATE TEMPORARY FUNCTION collect_all AS 'com.cloudera.exhibit.hive.CollectAllUDAF';
CREATE TEMPORARY FUNCTION collect_distinct AS 'com.cloudera.exhibit.hive.CollectDistinctUDAF';
CREATE TEMPORARY FUNCTION array_union AS 'com.cloudera.exhibit.hive.ArrayUnionUDF';
