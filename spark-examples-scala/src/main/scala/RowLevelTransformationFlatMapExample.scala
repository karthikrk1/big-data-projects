import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/24/17.
  * This file contains some example Row Level Transformation using flatMap()
  * for use with Spark
  */
object RowLevelTransformationFlatMapExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Row Level Transformation").setMaster("yarn-client")
    val sc = new SparkContext(conf)

    // Create a list
    val l = List("Hello", "How are you doing?", "let us do word count for this", "we will see how many times each word repeat")

    // Create a RDD using the parallelize method
    val lRdd = sc.parallelize(l)

    // apply flatMap to perform WordCount
    /*
    Why use flatMap.
    Map -> Output
    ("Hello")
    ("How", "are", "you", "doing")
    ("let", "us", "do", "word", "count", "for", "this")
    ("we", "will", "see", "how", "many", "times", "each", "word", "repeat")

    FlatMap -> Output
    Hello
    How
    are
    you
    doing
    let
    us
    do
    word
    count
    for
    this
    we
    will
    see
    how
    many
    times
    each
    word
    repeat

    -- Because of this we use flatMap and not map for this example
    -- flatMap is also used with groupByKey()
     */
    val wc = lRdd.flatMap(ele => ele.split(" "))
      .map(word => (word,1))
      .countByKey()
  }
}
