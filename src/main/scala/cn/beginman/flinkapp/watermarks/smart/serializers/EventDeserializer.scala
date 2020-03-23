package cn.beginman.flinkapp.watermarks.smart.serializers

import cn.beginman.flinkapp.watermarks.smart.model.EventModel
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

class EventDeserializer extends AbstractDeserializationSchema[EventModel] {

  override def deserialize(message: Array[Byte]): EventModel = {
    val msg = new String(message)
    try {
      parse(JSON.parseObject(msg))
    } catch {
      case e: Throwable => {
        println("解析失败: " + e.getMessage)
        new EventModel()
      }
    }
  }


  private def parse(data: JSONObject): EventModel = {
    val v = if (data.containsKey("v")) data.getString("v").toFloat else 0.0F
    val room =  data.getString("room")
    val id = data.getString("id").toLong
    val tm = data.getString("tm").toLong
    EventModel(v, room, id, tm)
  }

}
