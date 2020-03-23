package cn.beginman.flinkapp.watermarks.example1

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[String]): Unit = {
    val arr = ArrayBuffer[(String, Long)]()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val value = iterator.next()
      println(value._1 + " > " + (new DateTime((new Date(value._2))).toString("yyyy-MM-dd HH:mm:ss")))
      arr += value
      println(arr)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val timeWindow = context.window
      out.collect(key + "," + arr.size + ","
        + format.format(arr.head._2) + "," + format.format(arr.last._2) + ","
        + format.format(timeWindow.getStart) + "," + format.format(timeWindow.getEnd))
    }

  }
  }
