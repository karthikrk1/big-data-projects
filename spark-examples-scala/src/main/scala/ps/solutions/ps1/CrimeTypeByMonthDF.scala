package ps.solutions.ps1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Karthik Ramakrishnan on 7/29/18.
  */
object CrimeTypeByMonthDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Crime Type By Month DataFrames")
      .setMaster(args(0))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // for resolving implicit function calls
    import sqlContext.implicits._

    // Read the file
    val crimeData = sc.textFile(args(1))

    val header = crimeData.first()

    val crimeDataWithoutHeader = crimeData.filter(rec => rec != header)

    val crimeDataWithDateAndTypeDF = crimeDataWithoutHeader.
      map(r => (r.split(",")(2), r.split(",")(5))).
      toDF("crime_date", "crime_type")

    crimeDataWithDateAndTypeDF.registerTempTable("crime_data_krk")

    val finalDF = sqlContext.
      sql("select cast(concat(substr(crime_date, 7, 4), substr(crime_data, 0, 2)) as int) crime_month, " +
      "count(1) crime_count_per_month_and_type, " +
      "crime_type " +
      "from crime_data_krk " +
      "group by cast(concat(substr(crime_date, 7, 4), substr(crime_data, 0, 2)) as int), crime_type " +
      "order by crime_month, crime_count_per_month_and_type")

    finalDF.rdd.
      map(r => r.mkString("\t")).
      coalesce(1).
      saveAsTextFile(args(2), classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
