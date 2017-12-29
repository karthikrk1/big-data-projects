import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
/**
  * Created by Karthik Ramakrishnan on 12/28/17.
  */
object DataFrameExample2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("register temp table").
      setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // read RDD
    val ordersRDD = sc.textFile("/public/retail_db/orders")

    // to show stats
    ordersRDD.take(10).foreach(println)

    // convert to Data Frame from RDD

    val ordersDF = ordersRDD.map(order => {
      (order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))
    }).toDF

    val ordersDF2 = ordersRDD.map(order => {
      (order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))
    }).toDF("order_id", "order_date", "order_customer_id", "order_status")

    // see sample records and print the schema
    ordersDF.show

    ordersDF.printSchema

    // register as temp table

    ordersDF.registerTempTable("orders")

    // run SQL on temp Table

    sqlContext.sql("select * from orders").show

    sqlContext.sql("select order_status, count(order_status) as status_count from orders group by order_status").show

    val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList

    val productsRDD = sc.parallelize(productsRaw)

    val productsDF = productsRDD.map(p => {
      (p.split(",")(0).toInt, p.split(",")(2))
    }).toDF("product_id","product_name")

    productsDF.registerTempTable("products")

    sqlContext.sql("select * from products").show

  }

}
