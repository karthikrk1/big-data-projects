import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object SaveWithCompression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("saveAsTextFile with delimiters Example")
      .setMaster("yarn-client")

    val sc = new SparkContext(conf)

    val orders = sc.textFile("hdfs:///user/karthik/orders")

    // a simple API to count By status

    val countByStatus = orders.
      map(order => (order.split(",")(3),1)).
      reduceByKey((total, count) => total+count)

    // saving with compression
    // using classOf to specify the className of the compression codec
    countByStatus.saveAsTextFile("/user/karthik/order_count_example_comp", classOf[org.apache.hadoop.io.compress.SnappyCodec])

    // validation
    sc.textFile("/user/karthik/order_count_example_comp").collect.foreach(println)
  }
}
