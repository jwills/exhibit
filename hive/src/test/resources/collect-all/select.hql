SELECT orderid, collect_all(struct(quantity,orderdate,price,productid))
FROM orders
GROUP BY orderid
;
