import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample5 {
  val conf = new SparkConf().setAppName("Aggregation Example with aggregateByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  // total and max revenue in one iteration

  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")
  val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
  // I/p => (order_id, order_item_subtotal)
  // O/p => (order_id, (total_revenue, max_order_item_subtotal))
  val revenueAndMax = orderItemsMap.aggregateByKey((0.0f, 0.0f))(
    (inter, subtotal) => (inter._1 + subtotal, if(subtotal > inter._2) subtotal else inter._2),
    (total, inter) => (total._1 + inter._1, if(total._2 > inter._2) total._2 else inter._2)
  )
}
