import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Karthik Ramakrishnan on 12/23/17.
  */
object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample App").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    // to check all the spark conf
    // This can also be viewed in the tracking URL
    // which is usually found when running the spark-shell
    sc.getConf.getAll.foreach(println) // prints all conf line by line
    // Creating a RDD from HDFS
    val orders = sc.textFile("hdfs:///user/karthik/file")
    // get the first record of this RDD
    orders.first()
    // get the first 10 records
    orders.take(10)
    // Reading from a local file system
    // Read from local fs using Source.fromFile
    // get the lines and make a list.
    val raw = scala.io.Source.fromFile("/local/path").getLines.toList
    // convert this to a RDD using sc.parallelize
    val rawRDD = sc.parallelize(raw)
    // Reading top 10 from raw
    rawRDD.take(10)
    //count the rows in RDD
    rawRDD.count()
    // get all elements as an array
    rawRDD.collect() // never use because if size is huge than memory can
    // generate OOM
    //take Sample
    // first argument - withReplacement- can repeat,
    // second argument - size of sample
    rawRDD.takeSample(true, 100).foreach(println) // to print
    // ordered sample
    rawRDD.takeOrdered(10) // based on natural ordering
    // do not run foreach on RDD
    // rawRDD.foreach(println)
    // reason, RDD are distributed and foreach is run on local machine
  }
}
