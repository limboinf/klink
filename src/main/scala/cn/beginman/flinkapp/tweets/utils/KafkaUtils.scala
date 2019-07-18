package cn.beginman.flinkapp.tweets.utils

import cn.beginman.flinkapp.tweets.analytics.JsonResult
import cn.beginman.flinkapp.tweets.schema.CustomCountSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010


object KafkaUtils {

  def createSink[T <: JsonResult[_]](topic: String): FlinkKafkaProducer010[T] = {
    new FlinkKafkaProducer010[T]("localhost:9092", "klink", new CustomCountSchema[T])
  }
}
