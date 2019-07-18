/**
  * 窗口函数
  */
package cn.beginman.flinkapp.c3

import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowFunctions {

  def threshold = 25.0

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // 通过id分组, 每个窗口最小温度, Tuple(id, template)
    val minTempPerWindow: DataStream[(String, Double)] = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    // 通过id分组, 窗口函数计算最小温度
    val minTempPerWindow2: DataStream[(String, Double)] = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce(new MinTempFunction)

    // 通过id分组, 窗口函数计算平均值
    val avgTempPerWindow: DataStream[(String, Double)] = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .aggregate(new AvgTempFunction)

    // 通过id分组，窗口函数每5秒输出最高和最低温度
    val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction)

    val minMaxTempPerWindow2: DataStream[MinMaxTemp] = sensorData
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        // 逐步计算最小和最大温度
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        new AssignWindowEndProcessFunction()
      )

    minMaxTempPerWindow2.print()

    env.execute()
  }
}

/**
  * ReduceFunction 方法签名
  *   T reduce(T value1, T value2)
  */
class MinTempFunction extends ReduceFunction[(String, Double)] {
  override def reduce(r1: (String, Double), r2: (String, Double)) = {
    (r1._1, r1._2.min(r2._2))
  }
}

/**
  * AggregateFunction
  * interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable
  *  - IN: input value, 这里是(String, Double) 元祖
  *  - ACC: 累加器的类型（中间聚合状态）。 这里是(String, Double, Int) 元祖
  *  - OUT: 聚合结果类型，这里是(String, Double)元祖
  * 方法签名：
  */
class AvgTempFunction extends AggregateFunction[
    (String, Double),       // IN
    (String, Double, Int),  // ACC
    (String, Double)        // OUT
    ] {

  // 创建一个新的累加器，启动一个新的聚合。
  // 函数签名：
  // ACC createAccumulator();
  override def createAccumulator() = {
    ("", 0.0, 0)  // 这里初始化为：("id", 温度累加值, 总个数)
  }

  // 将给定的输入值添加到给定的累加器，返回新的累加器值。
  // 函数签名：
  // ACC add(IN value, ACC accumulator);
  override def add(in: (String, Double), acc: (String, Double, Int)) = {
    (in._1, in._2 + acc._2, 1 + acc._3)
  }

  // 从累加器中获取聚合结果
  // 函数签名：
  // OUT getResult(ACC accumulator);
  override def getResult(acc: (String, Double, Int)) = {
    (acc._1, acc._2 / acc._3)   // 求平均值
  }

  // 合并两个累加值，获取合并后的状态
  // 函数签名：
  // ACC merge(ACC a, ACC b);
  override def merge(acc1: (String, Double, Int), acc2: (String, Double, Int)) = {
    (acc1._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
  }
}

case class MinMaxTemp(id: String, min: Double, max:Double, endTs: Long)

/**
  * ProcessWindowFunction 计算最高和最低温度
  * 是一个抽象类，签名如下：
  * ProcessWindowFunction[IN, OUT, KEY, W <: Window]
  */
class HighAndLowTempProcessFunction
  extends ProcessWindowFunction[Sensor, MinMaxTemp, String, TimeWindow] {

  override def process(
                        key: String,    // keyBy的key
                        ctx: Context,   // 窗口上下文
                        vals: Iterable[Sensor],  // 需要计算的元素列表
                        out: Collector[MinMaxTemp]  // 输出的collector
                      ): Unit = {

    val temps = vals.map(_.temperature)
    val windowEnd = ctx.window.getEnd

    out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
  }
}

class AssignWindowEndProcessFunction
  extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {

  override def process(
                        key: String,
                        ctx: Context,
                        minMaxIt: Iterable[(String, Double, Double)],
                        out: Collector[MinMaxTemp]): Unit = {

    val minMax = minMaxIt.head
    val windowEnd = ctx.window.getEnd
    out.collect(MinMaxTemp(key, minMax._2, minMax._3, windowEnd))
  }
}