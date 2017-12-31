import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/26/17.
  */
object SaveAsTextFileExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("saveAsTextFile Example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val orders = sc.textFile(args(1))

    // Using countByKey will give a Map instead of RDD
    // and we cannot save
    // so using reduceByKey to get a RDD on which we can save

    val countByStatus = orders.map(order => (order.split(",")(3),1)).
      reduceByKey((total, count) => total+count)

    countByStatus.saveAsTextFile(args(2))

    // validation
    sc.textFile(args(2)).collect.foreach(println)
  }
}
