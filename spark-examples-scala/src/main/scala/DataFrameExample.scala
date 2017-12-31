import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
/**
  * Created by Karthik Ramakrishnan on 12/24/17.
  */
object DataFrameExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample DataFrame App").setMaster(args(0))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Create a DF using read API
    // Here example shows a JSON file
    // Other supported formats include Avro, ORC and Parquet
    val exDF = sqlContext.read.json(args(1))
    // preview the data
    exDF.show
    // Schema for DF
    exDF.printSchema
    // select only some columns in DF
    exDF.select("id","name").show
    // another API to create DF
    // load needs another argument to specify type
    // read has separate methods for each type.
    val anotherDF = sqlContext.load(args(2))
    // same operations can be applied
    // for example showing some preview data
    anotherDF.show
  }
}
