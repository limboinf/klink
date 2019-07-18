package cn.beginman.flinkapp.c2

import org.apache.flink.api.scala._
import cn.beginman.flinkapp.c2.util.SmokeLevel.SmokeLevel
import cn.beginman.flinkapp.c2.util.{Alert, SmokeLevel, SmokeLevelSource}
import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * 多流操作
  * 一个简单的监控报警应用程序
  * 该应用程序接收温度传感器读数流和烟雾水平测量流
  * 当温度超过给定的阈值并且烟雾水平很高时就报警
  *
  * links:
  * http://wuchong.me/blog/2016/05/20/flink-internals-streams-and-operations-on-streams
  */
object MultiStreamTransformations {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    // 摄取sensor流
    val tempReadings: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // 摄取smoke流
    val smokeReadings: DataStream[SmokeLevel] = env
      .addSource(new SmokeLevelSource)
      .setParallelism(1)

    // group sensor readings by their id
    val keyed: KeyedStream[Sensor, String] = tempReadings
      .keyBy(_.id)

    /**
      * 双流连接处理
      * connect操作创建的是ConnectedStreams或BroadcastConnectedStream
      * ConnectedStreams的flatMap操作接收 CoFlatMapFunction函数
      */
    val coStreams: ConnectedStreams[Sensor, SmokeLevel] = keyed
      .connect(smokeReadings.broadcast)

    val alerts = coStreams
      .flatMap(new RaiseAlertFlatMap)

    alerts.print()

    env.execute("Multi-Stream Transformations Example")
  }

  /**
    * 签名：CoFlatMapFunction<IN1, IN2, OUT>
    * 处理第一个(IN1, 这里是Sensor类型)input 和 第二个(IN2, 这里是SmokeLevel类型)的input
    */
  class RaiseAlertFlatMap extends CoFlatMapFunction[Sensor, SmokeLevel, Alert] {

    var smokeLevel = SmokeLevel.Low

    // 为第一个连接流中的每个元素调用此方法。
    // 如果Smoke级别最高其温度超过100则输出Alert类型数据
    override def flatMap1(in1: Sensor, collector: Collector[Alert]): Unit = {
      if (smokeLevel.equals(SmokeLevel.High) && in1.temperature > 100) {
        collector.collect(Alert(s"Risk of fire!, the temperature is: ${in1.temperature}", in1.timestamp))
      }
    }

    // 为第二个连接流中的每个元素调用此方法。
    // 先更新目前smoke级别
    override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
      smokeLevel = in2
    }
  }
}
