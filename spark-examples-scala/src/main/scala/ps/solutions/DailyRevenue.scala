package ps.solutions

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object DailyRevenue {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).
      setAppName("Daily Revenue")

    val sc = new SparkContext(conf)

    // Read the Orders and OrderItems files from HDFS
    val orders = sc.textFile(args(1))

    val orderItems = sc.textFile(args(2))

    // Filter for only Status = CLOSED and COMPLETE
    val ordersFiltered = orders.filter(order => order.split(",")(3) == "CLOSED" || order.split(",")(3) == "COMPLETE")

    // convert to PairedRDD
    val ordersMap = ordersFiltered.map(order => (order.split(",")(0).toInt, order.split(",")(1).substring(0, 10)))

    val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, (oi.split(",")(2).toInt, oi.split(",")(4).toFloat)))

    // Join on the common key
    val ordersJoinOrderItems = ordersMap.join(orderItemsMap)

    // Change key for reduceByKey
    val ordersJoinMap = ordersJoinOrderItems.map(o => ((o._2._1, o._2._2._1), o._2._2._2))

    // Find the daily revenue per Product
    val dailyRevenuePerProductId = ordersJoinMap.reduceByKey((revenue, order_item_subtotal) => revenue + order_item_subtotal)

    // Read Products file from local FS
    val productsList = scala.io.Source.fromFile(args(3)).getLines.toList

    // Use the parallelize method to convert to RDD
    val products = sc.parallelize(productsList)

    // Convert into PairedRDD
    val productsMap = products.map(prod => (prod.split(",")(0).toInt, prod.split(",")(2)))

    // Map to make the dailyrevenue have the same key as product
    val dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(r => (r._1._2, (r._1._1, r._2)))

    // Join on product Id key
    val joinedProds = dailyRevenuePerProductIdMap.join(productsMap)

    // Sort By key to sort on Order Date and descending Order daily revenue
    val dailyRevenuePerProductSorted = joinedProds.map(r => ((r._2._1._1, -r._2._1._2), (r._2._1._1, r._2._1._2, r._2._2))).sortByKey()

    // Get the final result in the order we need
    val finalResult = dailyRevenuePerProductSorted.map(r => r._2._1 + "," + r._2._2 + "," + r._2._3)

    // Save to local FS
    finalResult.saveAsTextFile(args(4)) // As text file
  }
}
