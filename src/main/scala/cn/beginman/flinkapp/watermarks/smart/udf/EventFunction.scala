package cn.beginman.flinkapp.watermarks.smart.udf

import cn.beginman.flinkapp.watermarks.smart.model.{EventModel, ResModel}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class EventFunction extends WindowFunction[EventModel, ResModel, String, TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[EventModel],
                     out: Collector[ResModel]): Unit = {

    val room = key.split("_")(0)
    val id = key.split("_")(1).toLong

    println("key: " + key + " windowStart: " + window.getStart + " windowEnd: " + window.getEnd)
    val minV = input.minBy(_.v).v
    val maxV = input.maxBy(_.v).v
    val tm = input.maxBy(_.tm).tm
    out.collect(ResModel(room, id, minV, maxV, tm))

  }
}
