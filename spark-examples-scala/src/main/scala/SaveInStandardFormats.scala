import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
class SaveInStandardFormats {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Save in standard formats Example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // Standard formats include:
    // json, orc, parquet and avro

    // Using save and load API
    val ordersDF = sqlContext.read.json(args(1))

    // save needs the type as argument
    ordersDF.save(args(2), "parquet")

    // load for validating using show method
    sqlContext.load(args(2), "parquet").show
  }
}
