package cn.beginman.flinkapp.tweets.schema

import cn.beginman.flinkapp.tweets.dao.EventData
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

/**
  * 自定义kafka反序列化
  */
class EventDeserializer extends AbstractDeserializationSchema[EventData] {

  override def deserialize(message: Array[Byte]): EventData = {

    val msg = new String(message)
    try {
      val data = JSON.parseObject(msg)
      val id = data.getInteger("id")
      val name = data.getString("name")
      val tm = data.getLong("tm")
      EventData(id, name, tm)
    } catch {
      case e: Throwable => {
        println(e.getCause)
        EventData(0, "", 0L)
      }
    }
  }

}
