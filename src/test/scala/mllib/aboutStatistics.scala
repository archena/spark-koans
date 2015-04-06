package mllib

import spark.koans._
import spark.koans.Blanks.__
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics;

/**
 * Koans on statistics
 */
class AboutStatistics extends FunSuite with Matchers with TestSparkContext {
  test("summary statistics") {
    // The trait MultivariateStatisticalSummary provides a number of useful statistics for a set of vectors
    val observations = sc.parallelize(Array(
      Vectors.dense(1.0, 4.0, 8.0),
      Vectors.dense(2.0, 8.0, 4.0),
      Vectors.dense(6.0, 9.0, 0.0)))
    val summary = Statistics.colStats(observations)

    summary.count should be(3)

    // n.b. these are all *column-wise* operations
    summary.max should be(Vectors.dense(6.0, 9.0, 8.0))
    summary.min should be(Vectors.dense(1.0, 4.0, 0.0))
    summary.numNonzeros should be(Vectors.dense(3, 3, 2))
    summary.mean should be(Vectors.dense(3.0, 7.0, 4.0))
    summary.variance should be(Vectors.dense(7.0, 7.0, 16.0))

    // l1 norm: sum of each column; the Manhattan norm
    summary.normL1 should be(Vectors.dense(9.0, 21.0, 12.0))

    // l2 norm: square root of sum of squares; the Euclidean norm
    summary.normL2 should be(Vectors.dense(6.4031242374328485, 12.68857754044952, 8.94427190999916))
  }

  test("correlations") {
    // If we have two series, we can compute the degree of correlation between them
    // both series must have the same cardinality (size) and number of partitions
    val sx = sc.parallelize(Array(1.0, 7.0, 8.0, 2.0, 5.0, 9.0, 8.0, 12.0, 5.0, 2.0, 3.0, 6.0, 10.0, 3.0, 0.0, 1.0))
    val sy = sc.parallelize(Array(1.0, 4.0, 7.0, 2.0, 5.0, 10.0, 11.0, 14.0, 10.0, 3.0, 3.0, 7.0, 10.0, 8.0, 2.0, 1.0))

    // The correlation is a single number representing how closely the two series correlate
    // 'pearson' is the default method. Spearman's method is also supported
    val corr = Statistics.corr(sx, sy, "pearson")
    corr should be(0.86 plusOrMinus 0.002)
  }
}
