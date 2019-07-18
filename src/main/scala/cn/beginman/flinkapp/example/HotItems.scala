package cn.beginman.flinkapp.example


import cn.beginman.flinkapp.util.{EnvApp, ProductEventSource, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/**
  *
  * 每隔5分钟输出过去一小时内点击量最多的前 N 个商品
  * http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items
  */

case class ItemViewCount(itemId: String,
                         windowEnd: Long, // 窗口结束时间戳
                         viewCount: Long  // 点击量
                        )

object HotItems extends EnvApp {

  def main(args: Array[String]): Unit = {

    // 统计业务时间上的每小时的点击量，所以要基于 EventTime 来处理
    val env = createEnv(TimeCharacteristic.EventTime)

    // 为了打印到控制台的结果不乱序，我们配置全局的并发为1
    env.setParallelism(1)

    val ds: DataStream[UserBehavior] = env.addSource(new ProductEventSource)

    // 指定如何获得业务时间，以及生成 Watermark
    // 这样就能得到带有时间标记的数据流了
    // PS:真实业务场景一般都是存在乱序的，所以一般使用 BoundedOutOfOrdernessTimestampExtractor
    val stream = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior] {
      override def extractAscendingTimestamp(event: UserBehavior): Long = {
        event.timestamp
      }
    })
    .filter(_.behavior == "pv") // 过滤出只有点击的数据

//    stream.print()

    stream.keyBy(_.itemId)  // 对itemId进行分组
    // 滑动窗口 窗口大小是1分钟，每隔10s滑动一次
    .timeWindow(Time.minutes(2), Time.seconds(20))
    // 增量的聚合操作，聚合掉数据，减少 state 的存储压力
    .aggregate(new CountAgg(), new WindowResultFunction())  // 得到了每个商品在每个窗口的点击量的数据流
      // 为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组
    .keyBy(_.windowEnd)
      // 自定义TopN函数
    .process(new TopNHotItems(3))
    .print()

    env.execute()
  }

}


/**
  * 统计窗口中的条数
  */
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = {
    accumulator + 1
  }

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
  * 将每个 key每个窗口聚合后的结果带上其他信息封装成ItemViewCount进行输出
  */
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, String, TimeWindow] {

  override def apply(key: String,   // key, itemId
                     window: TimeWindow,  // 窗口
                     aggregateResult: Iterable[Long], // 聚合函数的结果，即count的值
                     collector: Collector[ItemViewCount]  // 输出类型ItemViewCount
                    ): Unit = {
    val count = aggregateResult.iterator.next
    collector.collect(ItemViewCount(key, window.getEnd, count))
  }
}


/**
  * TopN函数
  * @param topSize topN
  */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
  // 保证在发生故障时，状态数据的不丢失和一致性
  // ListState 是 Flink 提供的类似 Java List 接口的 State API，它集成了框架的 checkpoint 机制，自动做到了 exactly-once 的语义保证
  var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 状态注册
    val itemsStateDesc: ListStateDescriptor[ItemViewCount] =
      new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])

    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  /**
    * 每当收到一条数据(ItemViewCount), 就注册一个 windowEnd + 1的定时器（Flink会忽略同一时间的重复注册)
    * windowEnd+1的定时器被触发时，意味着收到了windowEnd+1的Watermark，即收齐了该windowEnd下的所有商品窗口统计值
    * onTimer()处理收集的所有商品及点击量进行排序，选出topN, 并格式化输出
    * @param input
    * @param context
    * @param collector
    */
  override def processElement(input: ItemViewCount,
                              context: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    // 每条数据都保存到状态中
    itemState.add(input)
    // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有数据
    context.timerService().registerEventTimeTimer(input.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 获取收到的所有商品点击量
    var allItems = List[ItemViewCount]()
    itemState.get().iterator().asScala.foreach(item => allItems = item :: allItems)
    // 提前清除状态中的数据，释放空间
    itemState.clear()
    // 按照点击量从大到小排序
    allItems = allItems.sortBy(item => item.viewCount).reverse

    var res =
      s"""
          |=========== 时间：${timestamp-1} ==========
       """.stripMargin
    // for循环，防止越界（如窗口共1个元素小于topSize)情况
    for (i <- 1 to (topSize min allItems.size)) {
      val item: ItemViewCount = allItems(i)
      res = res +
        s"""
           |NO: $i | itemId: ${item.itemId} | PV : ${item.viewCount} """.stripMargin
    }

    out.collect(res)

  }
}