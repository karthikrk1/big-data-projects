import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object SetOperationsExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Set Operations Example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val orders = sc.textFile(args(1))

    val customer_201308 = orders.filter(order => order.split(",")(1).contains("2013-08")).map(order => order.split(",")(2).toInt)

    val customer_201309 = orders.filter(order => order.split(",")(1).contains("2013-09")).map(order => order.split(",")(2).toInt)

    // get customers who placed orders in Aug and Sep 2013
    // so use intersection

    val customersWhoPlacedOrdersInAugAndSep = customer_201308.
      intersection(customer_201309)

    customersWhoPlacedOrdersInAugAndSep.take(10).foreach(println)

    // get unique customers who placed orders in Aug or Sep 2013 -- union
    // union also needs distinct because it doesn't filter duplicates

    val customersWhoPlacedOrdersInAugOrSep = customer_201308.
      union(customer_201309).
      distinct

    customersWhoPlacedOrdersInAugOrSep.take(10).foreach(println)

    // get all customers who placed order in 2013 aug but not in sep 2013 (minus)
    // There is no API to do it
    // so we use left Outer Join and then filter out the values with None

    // Join needs PairedRDD and hence we create a dummy K,V using the
    // customer ID as Key and 1 as value
    val customer_201308Paired = customer_201308.map(c => (c,1))
    val customer_201309Paired = customer_201309.map(c => (c,1))

    val customersWhoPlacedInAugAndNotInSep = customer_201308Paired.
      leftOuterJoin(customer_201309Paired).
      filter(r => r._2._2 == None).
      map(r => r._1).
      distinct

    customersWhoPlacedInAugAndNotInSep.take(10).foreach(println)
  }
}
