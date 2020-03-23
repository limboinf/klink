package cn.beginman.flinkapp.watermarks.smart.model

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

case class ResModel(var room: String,
                    var id: Long,
                    var minV: Float,
                    var maxV: Float,
                    var tm: Long) {
  def toJson(t: ResModel): String = {
    write(t)(DefaultFormats)
  }
}
