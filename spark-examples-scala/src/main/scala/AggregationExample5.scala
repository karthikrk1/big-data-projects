import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Karthik Ramakrishnan on 12/25/17.
  */
object AggregationExample5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Aggregation Example with aggregateByKey")
      .setMaster(args(0))
    val sc = new SparkContext(conf)

    // total and max revenue in one iteration
    /*
    aggregateByKey is used when intermediate and final phase need different logic
    this can also be used when we want to do some complex logic like total
    revenue and max total in a single shot

    aggregateByKey needs careful examination

    aggregateByKey(initialization)(function)

    in the initialize phase initialize the output you desire. In this case a
    tuple of two floats for total revenue and max revenue

    in the logic phase
    the first is the intermediate result which is a tuple and subtotal which is
    the record

    the second is the final result and the intermediate result where both are
    tuples in this example

     */
    val orderItems = sc.textFile(args(1))
    val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
    // I/p => (order_id, order_item_subtotal)
    // O/p => (order_id, (total_revenue, max_order_item_subtotal))
    val revenueAndMax = orderItemsMap.aggregateByKey((0.0f, 0.0f))(
      (inter, subtotal) => (inter._1 + subtotal, if(subtotal > inter._2) subtotal else inter._2),
      (total, inter) => (total._1 + inter._1, if(total._2 > inter._2) total._2 else inter._2)
    )
    /*
    to implement min, the initialization needs to change else it will
    just show 0.0 for all the records.
    this needs to handle the 0.0 condition where when it is 0.0 we need
    to just take the total
    this is kind of a first step check
    */
  }

}
