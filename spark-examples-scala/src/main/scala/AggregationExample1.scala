import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Aggregation Example with countByKey")
      .setMaster(args(0))
    val sc = new SparkContext(conf)

    // extract order status and count by key
    // order status is the fourth column and hence array index 3
    // Schema:
    // order_id, customer_id, order_date, order_status
    val orders = sc.textFile(args(1))

    val ordersMap = orders.map(order => (order.split(",")(3),1)).countByKey

    ordersMap.foreach(println)
  }
}
