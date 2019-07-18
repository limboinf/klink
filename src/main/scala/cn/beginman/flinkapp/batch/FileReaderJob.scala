package cn.beginman.flinkapp.batch

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FileReaderJob {

  import org.apache.flink.api.scala.extensions._

  case class Point(x:Int, y:Int)

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(
      Point(1, 2),
      Point(3, 4),
      Point(5, 6)
    )

    input
        .filterWith {
          case Point(x, _) => x > 1
        }.reduceWith {
          case (Point(x1, y1), Point(x2, y2)) => Point(x1+x2, y1+y2)
        }.mapWith {
          case Point(x, y) => (x, y)
        }.flatMapWith {
          case (x, y) => Seq("x" ->x, "y" -> y)
        }.groupingBy {
          case (id, value) => id
        }

    env.execute()

  }

}
