package cn.beginman.flinkapp.tweets.streaming

import java.util.Properties

import cn.beginman.flinkapp.tweets.config.{AppConfig, EventStreamSource}
import cn.beginman.flinkapp.tweets.dao.EventData
import cn.beginman.flinkapp.tweets.schema.EventDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.{Logger, LoggerFactory}

/**
  * stream source 构造器
  * 目前支持：
  *   - kafka
  *   - ....
  */
object StreamFactory {

  lazy val log:Logger = LoggerFactory.getLogger(StreamFactory.getClass)

  def createStream(env: StreamExecutionEnvironment, props: AppConfig) = props.source match {
    case EventStreamSource.Kafka => kafkaEventStream(env, props)
    case _ => throw new Exception("只支持kafka")
  }

  def kafkaEventStream(env:StreamExecutionEnvironment, props: AppConfig): DataStream[EventData] = {
    log.info(s"Creating stream from Kafka. kafkaServers=${props.kafkaServers} kafkaTopic=${props.kafkaTopic}")
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", props.kafkaServers)
    kafkaProperties.setProperty("group.id", props.kafkaGroupId)
    val consumer = new FlinkKafkaConsumer010[EventData](props.kafkaTopic, new EventDeserializer(), kafkaProperties)
    consumer.setStartFromLatest()
    env.addSource(consumer)
  }

}
