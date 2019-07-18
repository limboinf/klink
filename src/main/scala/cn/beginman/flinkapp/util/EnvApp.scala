package cn.beginman.flinkapp.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * StreamExecutionEnvironment 公共方法
  */
trait EnvApp {

  def createEnv(t: TimeCharacteristic): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(t)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env
  }

}
