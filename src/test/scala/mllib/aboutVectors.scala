package mllib

import spark.koans._
import spark.koans.Blanks.__
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.mllib.linalg._

/**
 * Koans on vectors and matrices
 */
class AboutVectors extends FunSuite with Matchers {
  // MLlib provides special data types for vectors and matrices intended for use in machine learning applications.
  // These can be local (restricted to one machine) or distributed.

  test("local vectors") {
    // A dense vector is used to encode a vector that has few zeros
    val dense = Vectors.dense(1.0, 0.0, 3.0)
    dense.size should be(3)
    dense.toArray should be(Array(1.0, 0.0, 3.0))

    // A sparse vector is a more efficient encoding for vectors that have large runs of zeros in them
    // One way to create a sparse vector is by giving a list of non-zero indices, and the values they should take
    val sparse1 = Vectors.sparse(10, Array(0, 4, 5), Array(1.0, 2.0, 3.0))
    sparse1.size should be(__)
    sparse1.toArray should be(Array(1.0, __, __, __, __, __, __, __, __, __))

    // Another way is to give a sequence of (index, value) pairs
    val sparse2: Vector = Vectors.sparse(10, Seq((0, 1.0), (4, 2.0), (5, 3.0)))
    sparse2.size should be(sparse1.size)
    sparse2.toArray should be(sparse1.toArray)
  }

  test("local matrices") {
    // This shapes an array into a 3 row x 2 column matrix
    // The matrix is stored in column-major order - i.e. values in the same column are contiguous in the array
    val m = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    m.numRows should be(__)
    m.numCols should be(__)
    m(0, 0) should be(1.0)
    m(0, 1) should be(2.0)
    m(1, 0) should be(__)
    m(2, 1) should be(__)

    // A matrix can be transposed. Transposition exchanges columns and rows, e.g.
    // 1 2 3         1 4 7
    // 4 5 6  --->   2 5 8
    // 7 8 9         3 6 9
    m.transpose should be(Matrices.dense(2, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)))
  }
}
