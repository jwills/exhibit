Exhibit: It's SQL All The Way Down

This is a prototype collection of Hive UDFs that allow you to
treat the array fields within a Hive row as if they were mini-tables
and execute SQL statements (joins, aggregations, etc.) against them.
This is one of those things that seems really dumb when you first hear
about it, but allows you to do some interesting things that
would be difficult/impossible using regular SQL.

To get started, you're going to want to run:

	mvn clean package

...which will build an exhibit-<version>-jar-with-dependencies.jar file
inside of the target directory. Copy this file to a location that would
be convenient to load it into Hive, fire up a Hive shell, and then run
this:

	add jar exhibit-<version>-jar-with-dependencies.jar;
	create temporary function within as 'com.cloudera.exhibit.udtf.WithinUDTF';

Now you're ready to execute SQL inside of your SQL. For example, say that
you want to do a market basket analysis of some sales data you have in a table
that has a schema that looks like this:

	orderid: int
	orderdate: timestamp
	lineitems: array<struct<product_id: int, quantity: int, price: decimal>>

One of the steps in this market basket analysis involves a self-join so that you
can find out which products are frequently purchased together. One way to do this
self-join would be to flatten the nested line items using Hive's EXPLODE function
and then re-join the data based on the order identifier. Another, much faster way,
is to simply perform the self-join _within_ each row in Hive by treating the
lineitems array as if it was a tiny SQL table, and then performing the self-join
on just that data, like this:

	SELECT product_id1, product_id2, count(*) as cnt
	FROM orders
	LATERAL VIEW
	within("select t1.product_id as p1, t2.product_id as p2
	        FROM t1, t2 WHERE t1.product_id < t2.product_id",
		lineitems, lineitems) cooccurrences
	AS (product_id1, product_id2)
	GROUP BY product_id1, product_id2;

We're effectively writing a UDTF for Hive...in SQL. This is
surprisingly useful: SQL is the world's most popular statically typed
language, and we can verify the correctness and the return type of the
SQL declared inside of the `within` function before we kick off any
processing on the cluster.

Additionally, we're not limited to running a single SQL query inside of our
`within` function call: we can pass in an array of SQL queries, and Exhibit
will automatically store the result of each query into a temporary table (temp1,
temp2, etc.) so that those results can be referenced by subsequent queries for
additional processing. We can embed fairly complex SQL sessions, involving
multiple tables (a.k.a. arrays of records), processed in parallel for each
record in our Hive table, and the results of those queries can then be further
processed and aggregated using Hive's normal query processing.
