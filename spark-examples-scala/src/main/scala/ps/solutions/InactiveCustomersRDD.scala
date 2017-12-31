package ps.solutions
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Karthik Ramakrishnan on 12/30/17.
  */
object InactiveCustomersRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("inactive customers")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    /*
    This program is for finding the inactive customers; means
    customers who haven't placed any orders
    This will be done using RDD and left Outer Join and taking
    the details where the orders field is None
    */
    // Created RDDs
    val ordersRaw = scala.io.Source.fromFile(args(1)).getLines.toList
    val ordersRDD = sc.parallelize(ordersRaw)

    val customersRaw = scala.io.Source.fromFile(args(2)).getLines.toList
    val customersRDD = sc.parallelize(customersRaw)

    val ordersMap = ordersRDD.map(o => (o.split(",")(2).toInt, o))
    val customersMap = customersRDD.map(c => (c.split(",")(0).toInt, (c.split(",")(2), c.split(",")(1))))

    // Left outer join

    val customerJoinOrder = customersMap.leftOuterJoin(ordersMap)

    // remove where left Outer join value is None

    val filteredRDD = customerJoinOrder.filter(o => o._2._2 != None)

    val finalResult = filteredRDD.map(o => o._2._1._1 + ", " + o._2._1._2)

    finalResult.coalesce(1).saveAsTextFile(args(3))
  }
}
