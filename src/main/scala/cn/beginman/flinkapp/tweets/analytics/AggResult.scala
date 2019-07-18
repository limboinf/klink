package cn.beginman.flinkapp.tweets.analytics

import cn.beginman.flinkapp.tweets.config.AppConfig
import cn.beginman.flinkapp.tweets.dao.EventData
import cn.beginman.flinkapp.tweets.utils.KafkaUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AggResult extends PipelineResult[CustomCount] {

  override def name(): String = "agg"

  override def description(): String = "agg result"

  override def addToStream(stream: DataStream[EventData],
                           sinkFunction: SinkFunction[CustomCount],
                           props: AppConfig) = {

    val f: DataStream[CustomCount] = stream.filter(_.id > 20)
      .keyBy(_.name)
      .timeWindow(props.windowTime)
      .apply(new MyWindowsFunction)

    // debug
    // f.print()

    // 添加sink
    f.addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)

  }

  override def addToStream(stream: DataStream[EventData], props: AppConfig): Unit = {
    addToStream(stream, KafkaUtils.createSink[CustomCount](props.kafkaTopic), props)
  }

}


class MyWindowsFunction extends WindowFunction[EventData, CustomCount, String, TimeWindow] {

  override def apply(word:String,
                     window:TimeWindow,
                     vals: Iterable[EventData],
                     out: Collector[CustomCount]) = {

    val(cnt, sum) = vals.foldLeft((0, 0))( (c, r) => (c._1 + 1, c._2 + r.id))
    val tm = vals.maxBy(_.tm).tm
    val arg = sum / cnt

    out.collect(CustomCount("agg", "result", tm, arg))

  }
}
