import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample6 {
  val conf = new SparkConf().setAppName("Aggregation Example with sortByKey")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  val products = sc.textFile("hdfs:///public/retail_db/products")

  val productsMap = products.map(p => (p.split(",")(1).toInt, p))

  val productsSorted = productsMap.sortByKey()

  val productsSortedDesc = productsMap.sortByKey(false)

  // sort by product price after product category id

  val productsCompositeMap = products.filter(p => p.split(",")(4) != "").map(p => ((p.split(",")(1).toInt, p.split(",")(4).toFloat), p))

  val sortedProd = productsCompositeMap.sortByKey()

  // sort by desc product price after asc product category id

  val productsCompositeMap2 = products.filter(p => p.split(",")(4) != "").map(p => ((p.split(",")(1).toInt, -p.split(",")(4).toFloat), p))

  val sortedProd2 = productsCompositeMap2.sortByKey().map(p => p._2)
}
