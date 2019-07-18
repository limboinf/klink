package cn.beginman.flinkapp.c3

import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object SideOutputs {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val monitoredReadings: DataStream[Sensor] = readings
      .process(new FreezingMonitor)

    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()

    readings.print()

    env.execute()
  }
}

/** Emits freezing alarms to a side output for readings with a temperature below 32F. */
class FreezingMonitor extends ProcessFunction[Sensor, Sensor] {

  // define a side output tag
  lazy val freezingAlarmOutput: OutputTag[String] =
    new OutputTag[String]("freezing-alarms")

  override def processElement(
      r: Sensor,
      ctx: ProcessFunction[Sensor, Sensor]#Context,
      out: Collector[Sensor]): Unit = {
    // emit freezing alarm if temperature is below 32F.
    if (r.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}")
    }
    // forward all readings to the regular output
    out.collect(r)
  }
}
