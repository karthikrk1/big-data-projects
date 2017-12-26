import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object OuterJoinExample {
  val conf = new SparkConf().setAppName("Outer Join Example").setMaster("yarn-client")
  val sc = new SparkContext(conf)

  // Get all the orders which do not have corresponding entry in order_items

  // Read the files as RDDs
  val orders = sc.textFile("hdfs:///user/karthik/orders")

  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")

  // Create a PairedRDD for Join
  val ordersMap = orders.map(order => (order.split(",")(0).toInt, order))

  val orderItemsMap = orderItems.map(orderItem => (orderItem.split(",")(1).toInt, orderItem))

  // perform Left Outer Join
  val leftOuterJoinRDD = ordersMap.leftOuterJoin(orderItemsMap)

  /* Some tuple ops

  val t = leftOuterJoinRDD.first

  tuple => (1, (1,2))
  t._1 = 1
  t._2 = (1,2)
  t._2._2 = 2
  */

  // In order to get all the corresponding orders with no order Item
  // we check where the orderItem records are None
  // They are created as Option[T] which can have values Some or None
  // Some means some data exist
  // None is equivalent as in Python where there is no Data

  val noOrderItemData = leftOuterJoinRDD.filter(order => order._2._2 == None)

  val finalResult = noOrderItemData.map(order => order._2._1)

  finalResult.take(10).foreach(println)

  // Doing the same with right outer join
  // just the order varies
  // the tuple access also need to change

  val rightOuterJoinRDD = orderItemsMap.rightOuterJoin(ordersMap)

  val noOrderItemDataRight = rightOuterJoinRDD.filter(order => order._2._1 == None)

  val finalResultRight = noOrderItemDataRight.map(order => order._2._2)

  finalResult.take(10).foreach(println)
}
