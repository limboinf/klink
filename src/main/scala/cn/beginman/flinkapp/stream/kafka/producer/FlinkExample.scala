package cn.beginman.flinkapp.stream.kafka.producer

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010



object FlinkExample {

  import WordCount._

  val stopWords = Set("a", "an", "the")
  val window = Time.of(10, TimeUnit.SECONDS)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer = new FlinkKafkaConsumer010[String]("input", new SimpleStringSchema(), initProps())
    val lines = env.addSource(kafkaConsumer)
    val res = count(lines, stopWords, window)
    res
      .map(_.toString)
      .print()

    env.execute()

  }


  def initProps() :Properties = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("group.id", "flinkConsumer")

    prop
  }

}
