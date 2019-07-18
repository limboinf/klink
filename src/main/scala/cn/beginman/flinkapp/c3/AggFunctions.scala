package cn.beginman.flinkapp.c3

import org.apache.flink.api.scala._
import cn.beginman.flinkapp.util.{EnvApp, Sensor, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 聚会函数：基于位置(position, 从0开始）和字段
  * agg 与 aggBy的区别，如max 与maxBy
  * max返回最大值，而maxBy返回此字段中具有最大值的元素。
  */
object AggFunctions extends EnvApp {

  def main(args: Array[String]): Unit = {

    // 如果选择EventTime，则需要 assignTimestampsAndWatermarks 分配时间戳和水印
    val env = createEnv(TimeCharacteristic.ProcessingTime)
    val sensor: DataStream[Sensor] = env.addSource(new SensorSource)

    val tw = sensor
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))

    // 计算temperature总量
//    tw.sum("temperature").print()
//    tw.sum(2).print()

    // 计算最大值, 最小值
    tw.max("temperature").print()
    tw.max(2).print()
    tw.maxBy("temperature").print()
//    tw.min("temperature").print()
    env.execute()

  }
}
