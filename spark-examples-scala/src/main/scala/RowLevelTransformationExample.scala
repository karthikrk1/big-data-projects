import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/24/17.
  * This file contains some example Row Level Transformation using map()
  * for use with Spark
  */
object RowLevelTransformationExample {
  val conf = new SparkConf().setAppName("Row Level Transformation").setMaster("yarn-client")
  val sc = new SparkContext(conf)
  // Read a new RDD
  val orders = sc.textFile("hdfs:///user/karthik/orders")

  // We are transforming the entire row using map()
  // This is done across the entire RDD
  // each row in orders is now transformed and the result is stored in
  // orderDateAsInt
  val orderDateAsInt = orders.map((str: String) => {
    str.split(",")(1).substring(0, 10).replace("-","").toInt
  })

  // to view some sample result

  orderDateAsInt.take(10).foreach(println)

  // Most of the other APIs within Spark need a PairedRDD
  // with a Key,Value Pair and hence we create them as follows.
  // create K,V pair

  val ordersPairedRDD = orders.map(order => {
    val o = order.split(",")
    (o(0).toInt, o(1).substring(0, 10).replace("-","").toInt)
  })

  // Read a new RDD
  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")

  val orderItemsPairedRDD = orderItems.map(orderItem => {
    (orderItem.split(",")(1).toInt, orderItem)
  })

  // This can now be used to join the two RDDs on the common Key field
}
