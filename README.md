# Exhibit: It's SQL All the Way Down

Exhibit is an evolving collection of various projects for executing SQL against
things that look like tiny database tables, including:

1. Hive arrays of structs
2. Collections of Avro and Thrift records
3. Arrays of BSON objects from MongoDB

To get started, you'll want to run:

	mvn clean package

To build all of the jars, including the `exhibit-*-jar-with-dependencies.jar`
JAR that is useful for playing with Exhibit and Hive. There is more documentation
in the README file for the hive module.
