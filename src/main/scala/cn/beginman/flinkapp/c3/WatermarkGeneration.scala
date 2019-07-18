/**
  * 水印生成器
  */
package cn.beginman.flinkapp.c3

import cn.beginman.flinkapp.util.{Sensor, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

object WatermarkGeneration {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings: DataStream[Sensor] = env
      .addSource(new SensorSource)

    // 分配时间戳和周期性水印
    val readingsWithPeriodicWMs = readings
      .assignTimestampsAndWatermarks(new PeriodicAssigner)

    // 分配时间戳和间断性水印(punctuated watermarks)
    val readingsWithPunctuatedWMs = readings
      .assignTimestampsAndWatermarks(new PunctuatedAssigner)

    readingsWithPeriodicWMs.print()

    env.execute("Assign timestamps and generate watermarks")
  }
}

/**
  * 为记录分配时间戳, 提供1分钟无序边界的水印
  * 假定最大乱序时间间隔为1min (b), 所有被观察到的事件计算出的这些事件最大时间戳(t)
  * 发射一个时间落差为1min的水印，即此水印的时间戳为 t-1min (t-b)
  */
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[Sensor] {

  val bound: Long = 60 * 1000   // 1 min
  // 观察到的最大时间戳
  var maxTs: Long = _

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  // 抽取事件时间戳
  override def extractTimestamp(r: Sensor, previousTS: Long): Long = {
    maxTs = maxTs.max(r.timestamp)  // 更新最大时间戳
    r.timestamp     // 返回记录时间戳
  }
}

/**
  * 间歇性水印生成器
  * 计算某个条件来决定是否发射水印
  * 这里如果sensorId == "sensor_1" 则发射水印
  */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[Sensor] {

  val bound: Long = 60 * 1000 // 1min

  override def checkAndGetNextWatermark(r: Sensor, extractedTS: Long): Watermark = {
    if (r.id == "sensor_1") {
      new Watermark(extractedTS - bound)
    } else {
      null
    }
  }

  override def extractTimestamp(r: Sensor, previousTS: Long): Long = {
    r.timestamp
  }
}
