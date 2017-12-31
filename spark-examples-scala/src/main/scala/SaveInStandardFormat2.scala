import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object SaveInStandardFormat2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Save in standard formats Example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // Standard formats include:
    // json, orc, parquet and avro

    // Using write and read API
    val ordersDF = sqlContext.read.json(args(1))

    // write needs the type as argument
    ordersDF.write.orc(args(2))

    // read for validating using show method
    sqlContext.read.orc(args(3)).show
  }
}
