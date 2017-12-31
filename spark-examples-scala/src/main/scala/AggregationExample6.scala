import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Aggregation Example with sortByKey")
      .setMaster(args(0))
    val sc = new SparkContext(conf)
    // Problem: sort product by product category id

    // Read RDD
    val products = sc.textFile(args(1))

    // Create a PairedRDD
    val productsMap = products.map(p => (p.split(",")(1).toInt, p))

    // Sort based on key
    val productsSorted = productsMap.sortByKey()

    // Descending sort
    val productsSortedDesc = productsMap.sortByKey(false)

    // sort by product category id and then by product price

    val productsCompositeMap = products.filter(p => p.split(",")(4) != "").map(p => ((p.split(",")(1).toInt, p.split(",")(4).toFloat), p))

    val sortedProd = productsCompositeMap.sortByKey()

    // sort by product category id and then by product price
    // in descending order
    /*
    Here we just negate the second key and use and finally discard the key
    This cannot be applied to String which needs more complex processing
     */
    val productsCompositeMap2 = products.filter(p => p.split(",")(4) != "").map(p => ((p.split(",")(1).toInt, -p.split(",")(4).toFloat), p))

    val sortedProd2 = productsCompositeMap2.sortByKey().map(p => p._2)
  }

}
