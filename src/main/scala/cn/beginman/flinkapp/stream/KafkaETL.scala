package cn.beginman.flinkapp.stream

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaETL {

  case class Event(name:String)

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
//    require(params.has("kafka.topic"), "kafka topic required")
    val topic = params.get("kafka.topic", "qos_ping_history")

    // local运行时：createLocalEnvrionment方式，在同一JVM进程中运行，并启动任务监控web
    val conf: Configuration = new Configuration()
    conf.setString("rest.port", "9003")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 设置整个webUI可见
    env.getConfig.setGlobalJobParameters(params)

    val props:Properties = initProps(params)

    // 设置基本时间特性，有 ProcessingTime/EventTime/IngestionTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val consumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), props)
    // auto.offset.reset 也可配置
    consumer.setStartFromEarliest()
    val stream = env.addSource(consumer)
    stream.print()

    env.execute("kafkaETL")
  }

  def initProps(params: ParameterTool) :Properties = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", params.get("bootstrap.servers", "47.94.172.241:9092"))
    prop.setProperty("group.id", params.get("group.id", "flinkConsumerV1"))

    prop
  }

}
