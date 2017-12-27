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
    Part - 2: Use a scala collections API to work out a sample example
    for one record
    
     */
  }
}
