import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object SaveAsTextFileWithDelimiters {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("saveAsTextFile with delimiters Example")
      .setMaster("yarn-client")

    val sc = new SparkContext(conf)

    val orders = sc.textFile("hdfs:///user/karthik/orders")

    // Using countByKey will give a Map instead of RDD
    // and we cannot save
    // so using reduceByKey to get a RDD on which we can save

    val countByStatus = orders.map(order => (order.split(",")(3),1)).
      reduceByKey((total, count) => total+count)

    // use a map phase to convert with a delimter and then call save
    countByStatus.map(r => r._1 + "\t" + r._2).
      saveAsTextFile("/user/karthik/order_count_example_delim")

    // validating => read it back using sc.textFile and print sample records

    // Note: collect should not be used on huge datasets
    sc.textFile("/user/karthik/order_count_example_delim").
      collect.
      foreach(println)
  }
}
