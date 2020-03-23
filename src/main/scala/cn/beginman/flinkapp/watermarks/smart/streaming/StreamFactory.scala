package cn.beginman.flinkapp.watermarks.smart.streaming

import java.util.Properties

import cn.beginman.flinkapp.watermarks.smart.model.EventModel
import cn.beginman.flinkapp.watermarks.smart.serializers.EventDeserializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._

object StreamFactory {

  lazy val log: Logger = LoggerFactory.getLogger(StreamFactory.getClass)

  def kafkaEventStream(env: StreamExecutionEnvironment, topic: String, props: Properties): DataStream[EventModel] = {
    log.info(s"creating stream from kafka, $props")
    val consumer = new FlinkKafkaConsumer010[EventModel](topic, new EventDeserializer(), props)
    consumer.setStartFromLatest()
    env.addSource(consumer)
  }

}
