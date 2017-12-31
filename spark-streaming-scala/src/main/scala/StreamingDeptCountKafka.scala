import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
/**
  * Created by Karthik Ramakrishnan on 12/30/17.
  */
object StreamingDeptCountKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafka streaming dept count")
      .setMaster(args(0))

    val ssc = new StreamingContext(conf, Seconds(30))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667")

    val topicSet = Set("fkdemokrk")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    val messages = stream.map(s => s._2)

    val departmentMessages = messages.
      filter(msg => {
        val endpoint = msg.split(" ")(6)
        endpoint.split("/")(1) == "department"
      })

    val departments = departmentMessages.
      map(dm => {
        val endpoint = dm.split(" ")(6)
        val departmentName = endpoint.split("/")(2)
        (departmentName, 1)
      })

    val departmentTraffic = departments.reduceByKey(_+_)

    departmentTraffic.saveAsTextFiles("/user/karthikramakrishnan20/kafspark/cnt",".txt")

    ssc.start()
    ssc.awaitTermination()
  }
}
