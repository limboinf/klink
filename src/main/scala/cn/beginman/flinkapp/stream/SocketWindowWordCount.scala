package cn.beginman.flinkapp.stream

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified, please set '--port <port>'")
        return
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    // parse the data, group it, window it, and agg the count
    val windowWordCounts = text
      .flatMap { w => w.split("\\s")}
      .map { w => WordWithCount(w, 1)}
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // 单线程print
    windowWordCounts.print().setParallelism(1)

    env.execute("socket window wordcount")

  }

  case class WordWithCount(word: String, count: Long)

}
