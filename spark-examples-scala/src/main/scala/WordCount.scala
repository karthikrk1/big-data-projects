import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by Karthik Ramakrishnan on 12/18/17.
  *
  * This program is the Scala version of the WordCount program
  * to be run in Apache Spark
  *
  * Assumptions:
  *   This program assumes that the input file is delimited by space (" ")
  *   If this is not the case then the delimiter should be used in line 22
  *   inside flatMap
  *
  * Format to run:
  *
  * spark-submit --class WordCount --master [yarn/local] /path/to/jar <input-file> <output-file>
  *
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    // This makes sure INFO doesn't get shown. But for learning purposes
    // this step can be avoided to see how the job works in each step
    sc.setLogLevel("WARN")
    val inputFile = sc.textFile(args(0))
    val count = inputFile.flatMap(line => line.split(" "))
                            .map(word => (word,1))
                              .reduceByKey(_+_)
    count.saveAsTextFile(args(1))

  }
}
