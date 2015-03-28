import spark.koans._
import spark.koans.Blanks.__
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Koans on key-value pairs
 */
class AboutKeyValuePairs extends FunSuite with Matchers with TestSparkContext {
  // Spark works by manipulating 'Resilient Distributed Datasets' (RDDs).
  // these are fault-tolerant collections which can be operated on in parallel

  test("key-value pairs") {
    // A special class of RDDs deals with key-value pairs
    val cities = sc.parallelize(Array("Sheffield", "Nottingham", "Manchester", "Newcastle", "York", "Liverpool"))
    val lengths = cities.map(c => (c, c.length))
    lengths.keys.collect should be(cities.collect)
    lengths.values.collect should be(Array(9, 10, 10, 9, 4, 9))
  }

  test("operations on keys and values") {
    val pairs = sc.parallelize(Array(4 -> 5, 1 -> 7, 1 -> 12, 3 -> 5))

    // 'groupByKey' groups values according to their keys
    pairs.groupByKey.collectAsMap should be(Map(4 -> Seq(5), 1 -> Seq(7, 12), 3 -> Seq(5)))

    // 'mapValues' applies a function to the values only
    pairs.mapValues(v => v * 2).collectAsMap should be(Map(4 -> 10, 1 -> 14, 1 -> 24, 3 -> 10))

    // 'reduceByKey' applies a function to combine values having the same key
    pairs.reduceByKey((a, b) => a + b).collectAsMap should be(Map(4 -> 5, 1 -> 19, 3 -> 5))

    // 'sortByKey' sorts the RDD by its keys
    pairs.sortByKey().collectAsMap should be(Map(1 -> 7, 1 -> 12, 3 -> 5, 4 -> 5))
  }

  test("counting words") {
    val words = sc.parallelize(Array("red", "blue", "red", "red", "green", "white", "yellow", "white", "red", "green"))

    // Using 'map' and 'reduceByKey', implement a function to count the occurrences of each word
    def wordCount(input: RDD[String]): RDD[(String, Int)] = ???
    wordCount(words).collectAsMap should be(Map("yellow" -> 1, "green" -> 2, "red" -> 4, "white" -> 2, "blue" -> 1))
  }
}
