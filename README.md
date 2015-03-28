# Overview

A koan is an incomplete test. Complete it, and find enlightenment.

This is an interactive tutorial on [Apache Spark](spark.apache.org). There are a series of unit tests: some pass, illustrating simple concepts, while others fail, leaving you to write some code or fill in a gap. Wherever you see `__`, you must replace it with the correct answer. When prompted to write something more substantial, you'll be presented with an empty function body to fill in. Every test class has a Spark context called `sc` which is created by the `TestSparkContext` trait. You'll access Spark's functionality through this context.

While it may be possible to complete these exercises with no knowledge of Scala, it is assumed that you already have some familiarity with Scala and Scala collections.

Inspired by many other koan-style projects, which I guess all started with the [Ruby koans](http://rubykoans.com).

# Requirements

It should be possible to complete these exercises with only Scala and SBT installed. All dependencies, including Spark itself, should be downloaded by SBT.

# On Apache Spark

Apache Spark is an open source (Apache license) cluster computing engine. Put plainly, it's a tool for analysing large amounts of data in order to learn something about that data, and its strengths lie in its speed, versatility and language bindings. It can be used standalone or within Apache Hadoop and comes with bindings for Scala, Java and Python. Spark is designed with the intent of unifying batch processing, stream processing and interactive (query-based) analytics into one framework, which occur through its built-in libraries:</a></p>

* [Spark SQL](https://spark.apache.org/sql) - a SQL interface for querying structured data
* [Spark Streaming](https://spark.apache.org/streaming) - tools for processing real-time data streams
* [MLlib](https://spark.apache.org/mllib) - a collection of machine learning algorithms: classification, regression, clustering, etc
* [GraphX](https://spark.apache.org/graphx) - tools for analysing graphs (the vertex-edge kind)

# List of koans

## Manipulating RDDs (resilient distributed datasets)

```
sbt "testOnly AboutRDDs"
```

* Build an RDD from a parallelized collection
* Build an RDD from a file
* Partitioning
* Map, reduce and filter
* Counting
* Zipping
* Putting it together

## Using key-value pairs

```
sbt "testOnly AboutKeyValuePairs"
```

* Key-value pairs 
* Operations on keys and values

# Sources of inspiration

* [The Spark Programming Guide](http://spark.apache.org/docs/1.2.1/programming-guide.html)
* [Learn Scala with Koans](http://scalakoans.webfactional.com)


