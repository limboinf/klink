package cn.beginman.flinkapp.tweets.dao

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

case class EventData (id: Int,
                      name: String,
                      tm: Long) {

  def toJson(t: EventData): String = {
    write(t)(DefaultFormats)
  }
}
