This is a prototype collection of Hive UDFs that allow you to
treat the array fields within a Hive row as if they were mini-tables
and execute SQL statements (joins, aggregations, etc.) against them.
This is one of those things that seems really dumb when you first hear
about it, but allows you to do some interesting things that
would be difficult/impossible using regular SQL.

For example, say that
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
	within_table("select a.product_id as p1, b.product_id as p2
	    FROM t1 a, t1 b WHERE a.product_id < b.product_id",
	    lineitems) cooccurrences
	AS (product_id1, product_id2)
	GROUP BY product_id1, product_id2;

We're effectively writing a UDTF for Hive...in SQL. This is
surprisingly useful: SQL is the world's most popular statically typed
language, and we can verify the correctness and the return type of the
SQL declared inside of the `within` function before we kick off any
processing on the cluster.

Additionally, we're not limited to running a single SQL query inside of our
`within_table` function call: we can pass in an array of SQL queries, and Exhibit
will automatically store the result of each query into a temporary table (temp1,
temp2, etc.) so that those results can be referenced by subsequent queries for
additional processing. We can embed fairly complex SQL sessions, involving
multiple tables (a.k.a. arrays of records), processed in parallel for each
record in our Hive table, and the results of those queries can then be further
processed and aggregated using Hive's normal query processing.

The Exhibit Hive module provides the following functions for creating, maintaining,
and querying supernova schemas:

1. `within_table` (com.cloudera.exhibit.hive.WithinUDTF): The UDTF illustrated above, which is intended to be used as part
of LATERAL VIEW queries.
2. `within` (com.cloudera.exhibit.hive.WithinUDF): A UDF version of `within_table`, which can be deployed anywhere
a Hive UDF can be deployed. Returns an array of named structs.
3. `collect_all` (com.cloudera.exhibit.hive.CollectAllUDAF): A UDAF that can be used to create supernovas. It
can aggregate any Hive type, not just primitives like the built-in `collect_set` method. `collect_all` will
_not_ deduplicate identical values.
4. `collect_distinct` (com.cloudera.exhibit.hive.CollectDistinctUDAF): Like `collect_all`, but will deduplicate
aggregated values. Think of it like a version of `collect_set` that can aggregate complex types as well
as primitives.
5. `array_union` (com.cloudera.exhibit.hive.ArrayUnionUDF): Given two arrays of the same type, concatenate
their contents and return the unioned array. Will not deduplicate identical values.
6. `read_file` (com.cloudera.exhibit.hive.ReadFileUDF): A handy function that will read the contents of a
text file in Hive into an array of strings using an optional delimiter (the default is a semi-colon). Can
be used in conjunction with the `within_table` or `within` functions to separate your inner SQL from your
outer HQL.

