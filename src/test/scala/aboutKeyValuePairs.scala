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
  // A special class of RDDs deals with key-value pairs.
  //
  // n.b. these functions rely on implicit conversion of the RDD to PairRDDFunctions
  //      you must import org.apache.spark.SparkContext._ for them to work!

  test("key-value pairs") {
    val cities = sc.parallelize(Array("Sheffield", "Nottingham", "Manchester", "Newcastle", "York", "Liverpool"))
    val lengths = cities.map(c => (c, c.length))
    lengths.keys.collect should be(cities.collect)
    lengths.values.collect should be(Array(9, 10, 10, 9, 4, 9))
  }

  test("mapping values; reducing keys") {
    val pairs = sc.parallelize(Array(4 -> 5, 1 -> 7, 1 -> 12, 3 -> 5))

    // 'mapValues' applies a function to the values only
    pairs.mapValues(_ * 2).collectAsMap should be(Map(4 -> 10, 1 -> 14, 1 -> 24, 3 -> 10))

    // 'reduceByKey' applies a function to combine values having the same key
    pairs.reduceByKey(_ + _).collectAsMap should be(Map(4 -> 5, 3 -> 5, 1 -> __))
  }

  test("grouping by key") {
    val pairs = sc.parallelize(Array(4 -> 5, 1 -> 7, 1 -> 12, 3 -> 5))
    pairs.groupByKey.collectAsMap should be(Map(4 -> Seq(__), 1 -> Seq(__, __), 3 -> Seq(__)))
  }

  test("sorting by key") {
    val pairs = sc.parallelize(Array(4 -> 5, 1 -> 7, 1 -> 12, 3 -> 5))
    pairs.sortByKey().keys.collect should be(Array(__, __, __, __))
  }

  test("counting words") {
    val words = sc.parallelize(Array("red", "blue", "red", "red", "green", "white", "yellow", "white", "red", "green"))

    // Implement a function to count the occurrences of each word, ordering the results alphabetically by word
    def wordCount(input: RDD[String]): RDD[(String, Int)] = ???

    val count = wordCount(words)
    count.collectAsMap should be(Map("yellow" -> 1, "red" -> 4, "green" -> 2, "white" -> 2, "blue" -> 1))
    count.keys.collect should be(Array("blue", "green", "red", "white", "yellow"))
  }

  test("joins") {
    val peopleAges = sc.parallelize(Array(
      "bob" -> 21,
      "alex" -> 33,
      "jane" -> 28))
    val favouriteFood = sc.parallelize(Array(
      "bob" -> "pizza",
      "alex" -> "salad",
      "amy" -> "fish"))

    // An inner join: only keys which are common to both datasets are included
    peopleAges.join(favouriteFood).collectAsMap should be(Map(
      "alex" -> (33, "salad"),
      "bob" -> (21, "pizza")))

    // Left outer join: keys taken from the left dataset (ages). Values from right dataset are optional
    peopleAges.leftOuterJoin(favouriteFood).collectAsMap should be(Map(
      "alex" -> (33, Some("salad")),
      "jane" -> (28, None),
      "bob" -> (21, Some("pizza"))))

    // Right outer join: keys taken from right dataset (favourite foods). Values from left dataset are optional
    peopleAges.rightOuterJoin(favouriteFood).collectAsMap should be(Map(
      "alex" -> (Some(33), "salad"),
      "bob" -> (Some(21), "pizza"),
      "amy" -> (None, "fish")))
  }

  test("subtract by key (set difference) and co-group") {
    val setA = sc.parallelize(Array("sam" -> 10, "sarah" -> 8))
    val setB = sc.parallelize(Array("sam" -> 14))

    // This takes only the keys in the left set which are not present in the right set
    setA.subtractByKey(setB).collectAsMap should be(Map("sarah" -> 8))
    setB.subtractByKey(setA).collectAsMap should be(Map())
  }

  test("co-group") {
    val setA = sc.parallelize(Array("sam" -> 10, "sarah" -> 8))
    val setB = sc.parallelize(Array("sam" -> 14))

    // This combines two key-value sets, grouping values by key
    val cogroupAB = setA.cogroup(setB).collectAsMap
    cogroupAB.get("sam").get should be((Seq(10), Seq(14)))
    cogroupAB.get("sarah").get should be((Seq(8), Seq()))

    val cogroupBA = setB.cogroup(setA).collectAsMap
    cogroupBA.get("sam").get should be((Seq(14), Seq(10)))
    cogroupBA.get("sarah").get should be((Seq(), Seq(8)))
  }
}
