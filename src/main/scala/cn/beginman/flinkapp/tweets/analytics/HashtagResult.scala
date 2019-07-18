package cn.beginman.flinkapp.tweets.analytics

import cn.beginman.flinkapp.tweets.config.AppConfig
import cn.beginman.flinkapp.tweets.dao.EventData
import cn.beginman.flinkapp.tweets.utils.ElasticUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

object HashtagResult extends PipelineResult[CustomCount] {

  override def name(): String = "HashTagPipeline"

  override def description(): String = "hash result"

  override def addToStream(stream: DataStream[EventData],
                           sinkFunction: SinkFunction[CustomCount],
                           props: AppConfig): Unit = {

    val f = stream.filter(_.id > 10)
      .flatMap(h => h.name.split("s").map(l => CustomCount("name", l, h.tm, 1)))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .filter(_.total > 0)
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[EventData], props: AppConfig): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("event_data_2019_05", "event", props.elasticUrl), props)
  }

}
