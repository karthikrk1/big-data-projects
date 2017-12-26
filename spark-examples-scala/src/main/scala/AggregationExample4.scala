import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample4 {
  val conf = new SparkConf().setAppName("Aggregation Example with reduceByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  // using reduce By key to get the total
  // this can be used if both the combiner and reducer use same logic
  // as shown in the sum where both intermediate and final results
  // depend on the same sum logic
  // this can also be used to get the min and max as shown below
  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")
  val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  val revenuePerOrder = orderItemsMap.reduceByKey((total, revenue) => total + revenue)
  val minRevenuePerOrder = orderItemsMap.reduceByKey((min, revenue) => if(min < revenue) min  else revenue)
  // to check
  orderItemsMap.sortByKey().take(10).foreach(println)
  minRevenuePerOrder.sortByKey().take(10).foreach(println)
}
