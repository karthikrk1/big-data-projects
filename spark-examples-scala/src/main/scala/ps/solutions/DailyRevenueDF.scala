package ps.solutions
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
  * Created by Karthik Ramakrishnan on 12/28/17.
  */
object DailyRevenueDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(args(0)).
      setAppName("Daily Revenue Using DataFrame")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // for resolving implicit function calls
    import sqlContext.implicits._

    // connect to DB
    sqlContext.sql("use karthik_db_orc")

    // read local file
    val productsRaw = scala.io.Source.fromFile(args(1)).getLines.toList

    val productsRDD = sc.parallelize(productsRaw)

    val productsDF = productsRDD.map(p => {
      (p.split(",")(0).toInt, p.split(",")(2))
    }).toDF("product_id","product_name")

    productsDF.registerTempTable("products")

    sqlContext.setConf("spark.sql.shuffle.partitions","2")
    val daily_revenue = sqlContext.sql("select o.order_date, p.product_name, sum(oi.order_item_subtotal) as daily_revenue" +
      " from orders o inner join order_items oi" +
      " on o.order_id = oi.order_item_order_id" +
      " inner join products p " +
      " on p.product_id = oi.order_item_product_id" +
      " where o.order_status in ('COMPLETE', 'CLOSED')" +
      " group by o.order_date, p.product_name" +
      " order by o.order_date, daily_revenue desc")

    // Storing in Hive table

    sqlContext.sql("create database karthik_daily_revenue")

    sqlContext.sql("create table karthik_daily_revenue.daily_revenue " +
      "(order_date string, product_name string, daily_revenue float)" +
      "stored as orc")

    daily_revenue.insertInto("karthik_daily_revenue.daily_revenue")

  }
}
