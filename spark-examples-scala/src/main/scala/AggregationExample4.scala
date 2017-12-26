import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample4 {
  val conf = new SparkConf().setAppName("Aggregation Example with reduceByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  val orderItems = sc.textFile("hdfs:///public/retail_db/order_items")
  val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  val revenuePerOrder = orderItemsMap.reduceByKey((total, revenue) => total + revenue)
  val minRevenuePerOrder = orderItemsMap.reduceByKey((min, revenue) => if(min < revenue) min  else revenue)
  // to check
  orderItemsMap.sortByKey().take(10).foreach(println)
  minRevenuePerOrder.sortByKey().take(10).foreach(println)
}
