import spark.koans._
import spark.koans.Blanks.__
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.rdd.RDD

/**
 * Koans on the manipulation of Resilient Distributed Datasets, or RDDs
 */
class AboutRDDs extends FunSuite with Matchers with TestSparkContext {
  // Spark works by manipulating 'Resilient Distributed Datasets' (RDDs).
  // these are fault-tolerant collections which can be operated on in parallel.
  //
  // RDDs behave mostly like ordinary collections, but operations on them are parallelized.
  //
  // Optional reading: paper describing RDDs - https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia

  test("build an RDD from a parallelized collection") {
    // We can create an RDD backed by a collection
    val data = Array(5, 1, 7, 3)
    val distributedData = sc.parallelize(data)

    // The RDD contains precisely the same data as the collection
    // 'collect' gathers all values from the RDD into an Array, making it the reverse of 'parallelize' ('toArray' does the same thing)
    distributedData.collect should be(data)

    distributedData.count should be(data.size)
    distributedData.first should be(__)
  }

  test("build an RDD from a file") {
    // We can also load data directly from a file into an RDD
    val shopping = sc.textFile("data/shopping-list.txt")
    shopping.count should be(14)
  }

  test("partitioning") {
    // The data in an RDD are distributed among a number of partitions (sometimes referred to as 'slices')
    // These might reside on different machines. We can specify the number of partitions
    val data = sc.parallelize(1 to 100, 4)
    data.partitions.size should be(__)

    // It's also possible re-partition an RDD later on
    data.repartition(5).partitions.size should be(5)
  }

  test("map, reduce and filter") {    
    // 'map' applies an operation to every element in an RDD, producing a new one
    val words = sc.parallelize(Array("ginger", "garlic", "coriander", "cumin"))
    val lengths = words.map(s => s.length)
    lengths.collect should be(Array(6, 6, 9, __))

    // 'reduce' applies a function to combine all of the elements in an RDD into one value
    val totalLength = lengths.reduce((a, b) => a + b)
    totalLength should be(26)

    // 'filter' creates a new RDD which only includes elements which match a predicate
    // Fill in the predicate to remove strings with 6 or more characters 
    val shortWords = words.filter(???)
    shortWords.collect should be(Array("cumin"))
  }

  test("counting") {
    val numbers = sc.parallelize(Array(1, 7, 8, 2, 7))

    // 'count' just counts the elements in an RDD
    numbers.count should be(__)
    
    // 'distinct' gives only the distinct values of an RDD while respecting the order
    numbers.distinct.collect should be(Array(__, __, __, __))

    // 'countByValue' gives the frequencies of each unique value in a map
    numbers.countByValue should be(Map(1 -> 1, 2 -> 1, 7 -> 2, 8 -> 1))
  }

  test("zipping") {
    val names = sc.parallelize(Array("Amy", "Bob", "Tim"))
    val numbers = sc.parallelize(Array(27, 32, 21))

    // 'zip' combines two RDDs into one by pairing up corresponding elements
    names.zip(numbers).collect should be(Array(__ -> __, __ -> __, __ -> __))
  }

  test("house prices") {
    // Here are some house prices given in pound sterling.
    // Complete the function below so that it removes any that cost more than £150,000 and converts the prices to US dollars using the ratio given
    // Only return unique prices
    val houses = sc.parallelize(Array(120000, 125000, 500000, 150000, 450000, 120000, 134000))
    val poundsToDollars = 1.5

    def prices(input: RDD[Int]): RDD[Double] = ???

    prices(houses).collect should be(Array(201000.0, 225000.0, 180000.0, 187500.0))
  }
}
