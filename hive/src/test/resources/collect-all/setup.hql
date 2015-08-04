DROP TABLE IF EXISTS orders;

CREATE TABLE orders(
    orderid INT
  , orderdate INT
  , productid INT
  , quantity INT
  , price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'collect-all/orders.txt' INTO TABLE orders;

CREATE TEMPORARY FUNCTION collect_all AS 'com.cloudera.exhibit.hive.CollectAllUDAF';

