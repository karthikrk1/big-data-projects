import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
class SaveInStandardFormats {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Save in standard formats Example")
      .setMaster("yarn-client")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // Standard formats include:
    // json, orc, parquet and avro

    // Using save and load API
    val ordersDF = sqlContext.read.json("/user/krk_json/orders")

    // save needs the type as argument
    ordersDF.save("/user/karthik/orders_parquet", "parquet")

    // load for validating using show method
    sqlContext.load("/user/karthik/orders_parquet", "parquet").show
  }
}
