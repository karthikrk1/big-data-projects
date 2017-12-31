import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object GlobalRankingExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Global Ranking Example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    // details of top 5 products - globally

    // Read RDD
    val products = sc.textFile(args(1))

    // Convert to a RDD
    val productsMap = products.filter(p => p.split(",")(4) != "").
                      map(p => (p.split(",")(4).toFloat, p))

    // Sort in descending order based on the sortByKey
    val productSortedDesc = productsMap.sortByKey(false)

    // Use take to show the top 10. Note: This does not involve removing dups

    productSortedDesc.take(10).foreach(println) // this does not involve removing duplicates

    // to ignore duplicates, we need to invoke a Scala collections and perform sorting

    // doing using takeOrdered

    val productsAgain = sc.textFile(args(2))

    productsAgain.filter(p => p.split(",")(4) != "").takeOrdered(10)(Ordering[Float].reverse.on(p => p.split(",")(4).toFloat))
  }
}
