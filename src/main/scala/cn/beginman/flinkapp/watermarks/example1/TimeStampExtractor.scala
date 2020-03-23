package cn.beginman.flinkapp.watermarks.example1

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.joda.time.DateTime

/**
  * 核心的两行代码是：
  *
  * currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
  * new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  * 拿当前的时间和上一次的时间的最大时间，减去10s作为每次处理到来时的水印。
  *
  * 就是全局水印时间，是事件时间减去10s。
  */
class TimeStampExtractor extends AssignerWithPeriodicWatermarks[(String, Long)]
  with Serializable {

  var currentMaxTimestamp = 0L
  val maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
  var a: Watermark = null

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // 水印是当前时间减去10s
  override def getCurrentWatermark: Watermark = {
    a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    a
  }

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    val timestamp = element._2
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    // 控制台打印的数据（事件时间，最大时间，水印时间）
    println("(" + element._1 + "," + element._2 + "," + format.format(element._2) +
      ") | (" + currentMaxTimestamp + "," + format.format(currentMaxTimestamp) +
      ") | " + (new DateTime((new Date(a.getTimestamp))).toString("yyyy-MM-dd HH:mm:ss")))
    timestamp
  }
}
