package ps.solutions

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Karthik Ramakrishnan on 7/29/18.
  */
object CrimeTypeByMonthCoreAPI {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Crime Type By Month")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    // Read the file
    val crimeData = sc.textFile(args(1))

    val header = crimeData.first()

    val crimeDataWithoutHeader = crimeData.filter(rec => rec != header)

    val criminalRecordWithMonthAndType = crimeDataWithoutHeader.
      map(rec => {
        val r = rec.split(",")
        val d = r(2).split(" ")(0)
        val m = d.split("/")(2) + d.split("/")(0)
        ((m.toInt, r(5)),1)
      })

    // ReducByKey because we need RDDs to use another sorting logic.. so CountByKey is not the best option here

    val crimeCountPerMonthPerType = criminalRecordWithMonthAndType.
      reduceByKey((total, value) => total + value)

    val crimeCountPerMonthPerTypeSorted = crimeCountPerMonthPerType.
      map(r => ((r._1._1, -r._2), r._1._1.toString + "\t" +
      r._2.toString + "\t" + r._1._2.toString)).
      sortByKey()

    val finalResult = crimeCountPerMonthPerTypeSorted.map(r => r._2)

    finalResult.saveAsTextFile(args(2), classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
