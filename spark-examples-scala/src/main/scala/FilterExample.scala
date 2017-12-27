import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/24/17.
  * This example shows some filtering applied on the data
  * using filter API of Spark
  */
object FilterExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Filter Example").setMaster("yarn-client")
    val sc = new SparkContext(conf)

    /*
    The use case here is to filter the rows that have a status of
    "COMPLETE" or "CLOSED" and having order_date in the month of August 2013
     */

    // Read a new RDD
    val orders = sc.textFile("hdfs:///user/karthik/orders")

    // Get the distinct order status
    // This is needed in case there are some status synonymous to the
    // states we need
    // the fourth column in orders is order_status so we say array index 3
    val distinctStatus = orders.map(order => order.split(",")(3)).distinct

    // print and see each status
    // usually for debugging
    // collect should not be used on large datasets
    distinctStatus.collect.foreach(println)

    // Get the final result
    // We are using a variable to make it easier
    // The Boolean OR and AND are used to combine our use case
    val result = orders.filter(order => {
      val o = order.split(",")
      (o(3) == "COMPLETE" || o(3) == "CLOSED") && (o(1).contains("2013-08"))
    })

    // See some stats
    result.count()

    result.take(10).foreach(println)
  }

  
}
