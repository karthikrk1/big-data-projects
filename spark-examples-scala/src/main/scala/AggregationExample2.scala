import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample2 {
  val conf = new SparkConf().setAppName("Aggregation Example with reduce")
    .setMaster("yarn-client")
  val sc = new SparkContext(conf)

  val orderItems = sc.textFile("hdfs:///user/karthik/order_items")
  val orderItemsMap = orderItems.map(oi => oi.split(",")(4).toFloat)

  val totalRevenue = orderItemsMap.reduce((total, revenue) => total + revenue)

  val orderItemsMaxRevenue = orderItemsMap.reduce((max, revenue) => {
    if(max<revenue) revenue else max
  })

  val orderItemsMinRevenue = orderItemsMap.reduce((min, revenue) => {
    if(min<revenue) min else revenue
  })
}
