import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object RankingByKeyExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ranking By Key Example")
      .setMaster("yarn-client")

    val sc = new SparkContext(conf)

    /*
    get topN priced products with in each category
     */

    /*
     Part -1 Read products Data and convert to PairedRDD
             Apply groupByKey on the PairedRDD
      */
    val products = sc.textFile("hdfs:///user/karthik/products")

    val productsFiltered = products.filter(p => p.split(",")(4) != "")

    val productsMap = productsFiltered.map(p => (p.split(",")(1).toInt, p)) // (K, V) => (product_category_id , product)

    val productsGBK = productsMap.groupByKey()

    /*
    Part - 3: Use function from part-2 for solution
     */
    val top5ByCategory = productsGBK.flatMap(rec => getTopNPricedProducts(rec._2,5))

    top5ByCategory.count()

    top5ByCategory.take(100).foreach(println)
  }

  /*
  Part -2 : Write a generic function using Scala APIs
   */
  def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
    val prodUniqueList = productsIterable.map(p => p.split(",")(4).toFloat).toSet.toList
    val topNPrices = prodUniqueList.sortBy(p => -p).take(topN)

    val productsSorted = productsIterable.toList.sortBy(p => -p.split(",")(4).toFloat)
    val minTopN = topNPrices.min

    val topNPricedProducts = productsSorted.takeWhile(p => p.split(",")(4).toFloat >= minTopN)

    topNPricedProducts
  }
}
