import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample3 {
  val conf = new SparkConf().setAppName("Aggregation Example with groupByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  // groupByKey to group all the records by key
  // Here we use groupByKey to group all order_items by its order_ids
  // and perform sum to get per order total
  /*
  Notes:
  groupByKey is not as performant as the other two variants
  reduceByKey and aggregateByKey
  since it does not use a combiner
   */
  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")
  val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  val orderItemsGBK = orderItemsMap.groupByKey

  // get the revenue for each order id
  val perOrderRevenue = orderItemsGBK.map(r => (r._1, r._2.toList.sum))

  // get data in descending order by order_item_subtotal for each order_id
  // this gives result as follows:
  /*
  (2, 100.0)
  (2, 99.0)
  and so on based on each key.
   */
  val descOrder = orderItemsGBK.flatMap(r => {
    r._2.toList.sortBy(o => -o).map(k=> (r._1, k))
  })
}
