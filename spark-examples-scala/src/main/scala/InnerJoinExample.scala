import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object InnerJoinExample {
  val conf = new SparkConf().setAppName("Inner Join Example").setMaster("yarn-client")
  val sc = new SparkContext(conf)

  // Create RDDs
  val orders = sc.textFile("hdfs:///user/karthik/orders")

  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")

  // Make the RDDs as PairedRDDs for Join
  // This will give a (K,V) pair. where we join based on the common K
  val ordersMap = orders.map(order => (order.split(",")(0).toInt, order.split(",")(1).substring(0, 10)))

  val orderItemsMap = orderItems.map(orderItem => (orderItem.split(",")(1).toInt, orderItem.split(",")(4).toFloat))

  // Inner Join
  val joinRDD = ordersMap.join(orderItemsMap)

  // see some stats for verification
  joinRDD.take(10).foreach(println)

  joinRDD.count()
}
