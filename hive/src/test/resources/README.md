# Hive-Unit-Tests

To my knowledge, and much to my dismay, there is no good way to test Hive scripts/UDFs/etc.
There are quite a few one-off solutions to this problem out there, but none are ideal.
I've chosen the least evil of the lot: [Beetest](https://github.com/kawaa/Beetest).

## Usage notes:

```sh
# First download and build Beetest
# I'm linking to a branch with some downstream tweaks for the moment
$ cd /tmp # everyone develops code there, right?
$ git clone https://github.com/prateek/Beetest/tree/nested-type-support
$ cd Beetest
$ mvn -Pfull package

# Here's how you use it:
$ cd <path-to-exhibit>
$ mvn package # to build hive-jars
$ export EXHIBIT_HIVE_JAR=`pwd`/hive/target/exhibit-hive-0.6.0.jar
$ export BEETEST_BUILD_PATH=/tmp/Beetest/target
$ cd hive/src/test/resources
$ ./run-beetest.sh collect-all
```
