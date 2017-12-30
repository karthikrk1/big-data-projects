import org.apache.spark.SparkConf
import org.apache.spark.streaming._
/**
  * Created by Karthik Ramakrishnan on 12/30/17.
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val executionMode = args(0)
    val conf = new SparkConf().setAppName("streaming").setMaster(executionMode)
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream(args(1), args(2).toInt)

    val words = lines.flatMap(line => line.split(" "))

    val tuples = words.map(word => (word,1))

    val wordCount = tuples.reduceByKey((t,v) => t+v)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
