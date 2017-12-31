import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object SaveWithCompression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("saveAsTextFile with compression Example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val orders = sc.textFile(args(1))

    // a simple API to count By status

    val countByStatus = orders.
      map(order => (order.split(",")(3),1)).
      reduceByKey((total, count) => total+count)

    // saving with compression
    // using classOf to specify the className of the compression codec
    countByStatus.saveAsTextFile(args(2), classOf[org.apache.hadoop.io.compress.SnappyCodec])

    // validation
    sc.textFile(args(2)).collect.foreach(println)
  }
}
