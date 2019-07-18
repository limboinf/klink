/**
  * keyby 操作
  */
package cn.beginman.flinkapp.c2

import org.apache.flink.api.scala._
import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object KeyedTransformations {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // 通过id进行分组
    val keyed = readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))

    // 求最高温度
    val maxTempPerSensor: DataStream[Sensor] = keyed
      .reduce((r1, r2) => {
        if (r1.temperature > r2.temperature) r1 else r2
      })

    maxTempPerSensor.print()

    env.execute("Keyed Transformations Example")
  }

}
