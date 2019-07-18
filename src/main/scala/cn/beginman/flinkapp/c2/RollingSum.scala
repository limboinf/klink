package cn.beginman.flinkapp.c2

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object RollingSum {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val resultStream: DataStream[(Int, Int, Int)] = inputStream
      .keyBy(0) // key on first field of the tuple
      .sum(1)   // sum the second field of the tuple

    resultStream.print()

    // execute application
    env.execute("Rolling Sum Example")
  }

}
