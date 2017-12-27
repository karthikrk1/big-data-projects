import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/24/17.
  * This file contains some example String manipulation commands
  * for use with Spark
  */
object StringManipulationExamples {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("String Manipulation").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    // Read a new RDD
    val orders = sc.textFile("hdfs:///user/karthik/orders")
    // Get the first record => will be a String
    val str = orders.first
    // Split to form an array
    val arr =str.split(",")
    // the first column is Orderid, get it and convert to int from String
    val orderId = arr(0).toInt
    // check for contains of String. Returns boolean
    arr(1).contains("2013")
    // order date is the second column. Check for that in the array
    val orderDate = arr(1)
    // Returns only the date. Format of orderDate in input YYYY-MM-DD HH:MM:SS.T
    // Returns only YYYY-MM-DD
    orderDate.substring(0,10)
    val dateAlone = orderDate.substring(0,10)
    // Get only month from YYYY-MM-DD
    val monthAlone = dateAlone.substring(5,7)
    // get only HH:MM:SS.T from orderDate
    val timed = orderDate.substring(11)
    // replace character
    orderDate.replace('-','/')
    // replace string
    orderDate.replace("07","JUL")
    // find the index of the given string
    orderDate.indexOf("2")
    // find the index of the given string at the occurrence
    // the second argument is occurrence. This gives the index of 2 at its second
    // occurrence
    orderDate.indexOf("2",2)
    // length of string
    orderDate.length
    // check for empty string
    orderDate.isEmpty
    // convert string to lower case
    orderDate.toLowerCase
  }
}
