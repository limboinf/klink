package cn.beginman.flinkapp.watermarks.smart

import java.util.Properties

import cn.beginman.flinkapp.watermarks.smart.model.EventModel
import cn.beginman.flinkapp.watermarks.smart.streaming.StreamFactory
import cn.beginman.flinkapp.watermarks.smart.udf.EventFunction
import cn.beginman.flinkapp.watermarks.smart.watermarks.EventTimeAssigner
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010


object MainApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用EventTime作为基础时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // kafka 配置
    val ZOOKEEPER_HOST = "localhost:2181"
    val KAFKA_BROKERS = "localhost:9092"
    val TRANSACTION_GROUP = "flink-test"
    val TOPIC_NAME = "flink_wm_test"
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    val stream: DataStream[EventModel] = StreamFactory
      .kafkaEventStream(env, TOPIC_NAME, kafkaProps)
      .assignTimestampsAndWatermarks(new EventTimeAssigner(5))
    stream.print()

    val res = stream
      .keyBy(x=> x.room + "_" + x.id)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
      .apply(new EventFunction())

    res.print()

//    val producer = new FlinkKafkaProducer010[String](KAFKA_BROKERS, "pingRes", new SimpleStringSchema)
//    res.map(x=> x.toJson(x)).addSink(producer)
    env.execute("testJob")

  }


}
