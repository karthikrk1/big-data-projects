import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Aggregation Example with reduce")
      .setMaster(args(0))
    val sc = new SparkContext(conf)

    val orderItems = sc.textFile(args(1))
    val orderItemsMap = orderItems.map(oi => oi.split(",")(4).toFloat)

    // Reduce acts globally on all data and hence we may not be able
    // to use this with a per Key basis
    // Also this is an Action and hence this triggers execution
    // Reduce takes a function of type () and gives a scalar value
    val totalRevenue = orderItemsMap.reduce((total, revenue) => total + revenue)

    // Doing a similar logic for Max and min
    val orderItemsMaxRevenue = orderItemsMap.reduce((max, revenue) => {
      if(max<revenue) revenue else max
    })

    val orderItemsMinRevenue = orderItemsMap.reduce((min, revenue) => {
      if(min<revenue) min else revenue
    })
  }
}
