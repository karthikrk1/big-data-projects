import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample3 {
  val conf = new SparkConf().setAppName("Aggregation Example with groupByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")
  val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  val orderItemsGBK = orderItemsMap.groupByKey

  // get the revenue for each order id
  val perOrderRevenue = orderItemsGBK.map(r => (r._1, r._2.toList.sum))

  // get data in descending order by order_item_subtotal for each order_id

  val descOrder = orderItemsGBK.flatMap(r => {
    r._2.toList.sortBy(o => -o).map(k=> (r._1, k))
  })
}
