package cn.beginman.flinkapp.watermarks.example1

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.joda.time.format.DateTimeFormat
import org.apache.flink.api.scala._


object Example {

  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // kafka 相关参数
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("zookeeper.connect", "localhost:2181")
    prop.setProperty("group.id", "default_group_id")
    val consumer = new FlinkKafkaConsumer010[String]("flink-test", new SimpleStringSchema, prop)
    // 从上一次读取的位置开始读
    consumer.setStartFromGroupOffsets()

    // 输入数据的格式：0001,2019-11-12 11:25:00
    env.addSource(consumer)
      .filter(!_.isEmpty)
      .map(f => {
        val arr = f.split(",")
        val code = arr(0)
        val time = parseDateNewFormat(arr(1))
        (code, time)
      })
      .assignTimestampsAndWatermarks(new TimeStampExtractor)
    // // 按照 code 来逻辑划分窗口，并行计算
      .keyBy(_._1)
    // 指定 翻滚窗口，3s生成一个窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      // 允许延迟5s之后才销毁计算过的窗口
      //.allowedLateness(Time.seconds(5))
      // 处理窗口数据
      .process(new MyProcessWindowFunction)
      // 打印处理完的数据
      .print()

    env.execute()

  }

  def parseDateNewFormat(dt: String) : Long = {
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(dt).getMillis
  }

}
