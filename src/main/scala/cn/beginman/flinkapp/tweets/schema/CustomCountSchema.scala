package cn.beginman.flinkapp.tweets.schema

import cn.beginman.flinkapp.tweets.analytics.{CustomCount, JsonResult}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}

class CustomCountSchema[T <: JsonResult[_]] extends SerializationSchema[T] {

  override def serialize(element: T): Array[Byte] = {
    element.toJson().toString.getBytes()
  }
}
