package cn.beginman.flinkapp.processFn

import cn.beginman.flinkapp.util.{EnvApp, Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

object CountWithTm {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val stream: DataStream[Sensor] = env.addSource(new SensorSource)
//    stream.assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val result = stream
      .keyBy(_.id)
      .process(new CountWithTimeoutFunction)

    result.print()
    env.execute()

  }

}

// state中保存的数据类型
case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

class CountWithTimeoutFunction extends KeyedProcessFunction[String, Sensor, (String, Long)] {

  lazy val state: ValueState[CountWithTimestamp] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))

  override def processElement(sensor: Sensor,
                              ctx: KeyedProcessFunction[String, Sensor, (String, Long)]#Context,
                              out: Collector[(String, Long)]) = {

    val current: CountWithTimestamp = state.value match {
      case null => CountWithTimestamp(sensor.id, 1, sensor.timestamp)
      case CountWithTimestamp(key, count, lastModified) => CountWithTimestamp(key, count + 1, sensor.timestamp)
    }

    state.update(current)
    // 从当前事件时间开始计划下一个5秒的定时器
    ctx.timerService.registerProcessingTimeTimer(current.lastModified + 5000)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, Sensor, (String, Long)]#OnTimerContext,
                       out: Collector[(String, Long)]) = {

    state.value match {
      case CountWithTimestamp(key, count, lastModified)
        if timestamp == lastModified + 5000 => {
        out.collect((key, count))
      }
      case _ =>
    }
  }
}
