package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  test("should group postings") {
    val overflow = new StackOverflow()
    val value = overflow.groupedPostingsBad(overflow.rawPostings(StackOverflow.loadLines()))
    val tuples = value.take(100)
    tuples.foreach(println);

    //    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("should count HighScores") {
    val overflow = new StackOverflow()
    val grouped = overflow.groupedPostingsBad(overflow.rawPostings(StackOverflow.loadLines()))
    val hs = overflow.scoredPostings(grouped);
    val res = hs.take(100)
    res.foreach(println);

    //    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("should do vectors") {
    val overflow = new StackOverflow()
    val grouped = overflow.groupedPostings(overflow.rawPostings(StackOverflow.loadLines()))
    val hs = overflow.scoredPostings(grouped);
    val res = overflow.vectorPostings(hs);
    val f100 = res.take(100)
    f100.foreach(println);
    assert(res.count() == 2121822);

    //    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


}
