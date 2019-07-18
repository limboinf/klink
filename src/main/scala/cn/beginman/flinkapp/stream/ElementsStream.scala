/**
  * 通过 fromElements创建DataStream
  */
package cn.beginman.flinkapp.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object ElementsStream {

  case class Stock(price:Int, volume: Int) {
    override def toString: String = price.toString + "_" + volume.toString
  }

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input:DataStream[Stock] = env.fromElements("100, 1010", "120,700")
        .map(
          x => {
            val v: Array[String] = x.split(",")
            Stock(v(0).trim.toInt, v(1).trim.toInt)
          }
        )

    // input.keyBy(_.price)
    input.setParallelism(1).print()
    // execute application
    env.execute("Rolling Sum Example")
  }

}
