package cn.beginman.flinkapp.example

import cn.beginman.flinkapp.utils.WordCountData
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * WordCount示例
  * Usage:
  * {{{
  *    WordCount --input <path> --output <path>
  * }}}
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    val env = ExecutionEnvironment.getExecutionEnvironment
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val text =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Use --input to specify file input.")
        env.fromCollection(WordCountData.WORDS)
      }

    // flatMap 传递function, 取出一个元素，产生零个，一个，或者多个元素
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty}}
      // 操作每个元素，取出一个元素，产生一个元素, 这里对每个word产出(word, 1) Tuple
      .map{ (_, 1)}
      .groupBy(0)
      .sum(1)

    if (params.has("output")) {
      counts.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("wordCount")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      // 注意，print()方法自动会调用execute()方法，所以不需要 env.execute
      counts.print()
    }
  }

}
