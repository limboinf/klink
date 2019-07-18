/**
  * Flink的Process Function(低层次操作)
  *
  * stream.keyBy(...).process(new MyProcessFunction())
  */
package cn.beginman.flinkapp.c3

import cn.beginman.flinkapp.util.{Sensor, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object CoProcessFunctionTimers {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val filterSwitches: DataStream[(String, Long)] = env
      .fromCollection(Seq(
        ("sensor_2", 10 * 1000L), // forward readings of sensor_2 for 10 seconds
        ("sensor_7", 60 * 1000L)) // forward readings of sensor_7 for 1 minute)
      )

    val readings: DataStream[Sensor] = env.addSource(new SensorSource)

    val forwardedReadings = readings
      // connect操作符，返回一个ConnectedStreams[IN1, IN2], 该类型也有map, process, keyBy等操作
      // 这里返回ConnectedStreams[Sensor, (String, Long)]
      .connect(filterSwitches)
      // keyBy(Sensor.id, (String,Long) Tuple的第一个元素，根据sensor ids分组
      // ConnectedStreams keyBy之后返回的还是ConnectedStreams[IN1, IN2]类型
      .keyBy(_.id, _._1)
      // 调用process, 返回DataStream
      // process函数参数为 CoProcessFunction, 在连接的输入流上应用给定的[[CoProcessFunction]]，从而创建转换后的输出流。
      .process(new ReadingFilter)

    forwardedReadings.print()

    env.execute("Monitor sensor temperatures.")
  }
}

/**
  * CoProcessFunction 抽象类，处理两个流的元素并生成单个输出
  * 签名：CoProcessFunction<IN1, IN2, OUT>
  */
class ReadingFilter extends CoProcessFunction[Sensor, (String, Long), Sensor] {

  lazy val forwardingEnabled: ValueState[Boolean] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
    )

  lazy val disableTimer: ValueState[Long] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

  // 处理流1
  override def processElement1(
      reading: Sensor,
      ctx: CoProcessFunction[Sensor, (String, Long), Sensor]#Context,
      out: Collector[Sensor]): Unit = {

    if (forwardingEnabled.value()) {
      out.collect(reading)
    }
  }

  // 处理流2
  override def processElement2(
      switch: (String, Long),
      ctx: CoProcessFunction[Sensor, (String, Long), Sensor]#Context,
      out: Collector[Sensor]): Unit = {

    // enable reading forwarding
    forwardingEnabled.update(true)
    // set disable forward timer
    val timerTimestamp = ctx.timerService().currentProcessingTime() + switch._2
    val curTimerTimestamp = disableTimer.value()
    if (timerTimestamp > curTimerTimestamp) {
      // remove current timer and register new timer
      ctx.timerService().deleteEventTimeTimer(curTimerTimestamp)
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }
  }

  override def onTimer(
      ts: Long,
      ctx: CoProcessFunction[Sensor, (String, Long), Sensor]#OnTimerContext,
      out: Collector[Sensor]): Unit = {

    // remove all state. Forward switch will be false by default.
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}