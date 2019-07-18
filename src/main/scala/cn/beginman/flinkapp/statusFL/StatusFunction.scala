package cn.beginman.flinkapp.statusFL

import cn.beginman.flinkapp.util.{EnvApp, Sensor, SensorSource}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

/**
  * 状态管理
  * 下面的代码示例演示如何实现连续计数器，
  * 该计数器计算某个键的元素出现次数，并在每次出现时为该元素发出更新的计数。
  *
  */
object StatusFunction extends EnvApp {

  def main(args: Array[String]): Unit = {

    val env = createEnv(TimeCharacteristic.ProcessingTime)
    val source: DataStream[Sensor] = env.addSource(new SensorSource)

    source
      .keyBy(_.id)
      .map(new RichMapFunction[Sensor, (String, Long)] {

        private var status: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // 创建状态描述符
          val sensorIdCountDescriptor = new ValueStateDescriptor[Long]("counter", classOf[Long])
          // 获取状态句柄
          status = getRuntimeContext.getState[Long](sensorIdCountDescriptor)
        }

        override def map(value: Sensor): (String, Long) = {
          // key每次出现一次就累加
          val count = status.value() + 1
          status.update(count)
          (value.id, count)
        }
      })
      .print()

    env.execute()
  }
}
