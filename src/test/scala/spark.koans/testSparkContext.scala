package spark.koans

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkContext

trait TestSparkContext extends Suite with BeforeAndAfterAll {
  var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "Testing", System.getenv("SPARK_HOME"))
  }

  override def afterAll() {
    sc.stop()
  }
}
