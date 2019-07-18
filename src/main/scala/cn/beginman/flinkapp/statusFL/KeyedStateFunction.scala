package cn.beginman.flinkapp.statusFL

import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * state应用
  * 如果sensor温度与上次读数相比变化超过配置的阈值，则会发出警报。
  */
object KeyedStateFunction {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val keyedSensorData: KeyedStream[Sensor, String] = sensorData.keyBy(_.id)

    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
      .flatMap(new TemperatureAlertFunction(10.7))

    /* Scala shortcut to define a stateful FlatMapFunction. */
//    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
//      .flatMapWithState[(String, Double, Double), Double] {
//        case (in: Sensor, None) =>
//          // no previous temperature defined. Just update the last temperature
//          (List.empty, Some(in.temperature))
//        case (r: Sensor, lastTemp: Some[Double]) =>
//          // compare temperature difference with threshold
//          val tempDiff = (r.temperature - lastTemp.get).abs
//          if (tempDiff > 1.7) {
//            // threshold exceeded. Emit an alert and update the last temperature
//            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
//          } else {
//            // threshold not exceeded. Just update the last temperature
//            (List.empty, Some(r.temperature))
//          }
//      }

    alerts.print()

    env.execute("Generate Temperature Alerts")
  }
}

/**
  * 如果sensor温度与上次读数相比变化超过配置的阈值，则会发出警报。
  *
  * @param threshold The threshold to raise an alert.
  */
class TemperatureAlertFunction(val threshold: Double)
    extends RichFlatMapFunction[Sensor, (String, Double, Double)] {

  // 状态句柄对象
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 创建状态描述符(state descriptor)
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    // 获取状态句柄(state handle)
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  override def flatMap(reading: Sensor, out: Collector[(String, Double, Double)]): Unit = {

    // 从状态里取上次的温度值
    val lastTemp = lastTempState.value()
    // 如果当前温度与上次的差值大于指定的阈值则报警
    val tempDiff = (reading.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      out.collect((reading.id, reading.temperature, tempDiff))
    }

    // 更新状态
    this.lastTempState.update(reading.temperature)
  }
}
