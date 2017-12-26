import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample1 {
  val conf = new SparkConf().setAppName("Aggregation Example with countByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  // extract order status and count by key
  val orders = sc.textFile("hdfs:///public/retail_db/orders")

  val ordersMap = orders.map(order => (order.split(",")(3),1)).countByKey

  ordersMap.foreach(println)
}
