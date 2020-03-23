package cn.beginman.flinkapp.watermarks.smart.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

case class EventModel(var v: Float,
                     var room: String,
                     var id: Long = 0L,
                     var tm: Long) {
  def this() = this(0.0F, "", 0L, 0L)

  def toJson(t: EventModel): String = {
    write(t)(DefaultFormats)
  }
}
