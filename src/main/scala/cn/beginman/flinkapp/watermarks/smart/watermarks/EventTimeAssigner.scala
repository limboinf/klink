package cn.beginman.flinkapp.watermarks.smart.watermarks

import java.text.SimpleDateFormat
import java.util.Date

import cn.beginman.flinkapp.watermarks.smart.model.EventModel
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


class EventTimeAssigner(delay: Long, move: Long = 0) extends AssignerWithPunctuatedWatermarks[EventModel] with Serializable {
  val maxOutOfOrderness = delay
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  var lastWm: Watermark = null

  override def checkAndGetNextWatermark(lastElement: EventModel, extractedTimestamp: Long): Watermark = {
    val wm = new Watermark(extractedTimestamp - maxOutOfOrderness)
    println("Watermark： "+format.format(wm.getTimestamp)+" tm:"+format.format(extractedTimestamp))
    wm
  }

  override def extractTimestamp(element: EventModel, previousElementTimestamp: Long): Long = {
    var timestamp = element.tm
    val curr = new Date().getTime + move
    if (timestamp > curr) {
      timestamp = curr - move
    }
    timestamp
  }
}
