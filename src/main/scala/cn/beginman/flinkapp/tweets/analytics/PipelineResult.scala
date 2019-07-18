package cn.beginman.flinkapp.tweets.analytics

import cn.beginman.flinkapp.tweets.config.AppConfig
import cn.beginman.flinkapp.tweets.dao.EventData
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.slf4j.{Logger, LoggerFactory}

trait PipelineResult[R <: JsonResult[_]] {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def name(): String
  def description():String
  def addToStream(stream: DataStream[EventData], sinkFunction: SinkFunction[R], props: AppConfig)
  def addToStream(stream: DataStream[EventData], props: AppConfig)
}


object DataStreamImplicits {
  implicit class Implicits(ds: DataStream[EventData]) {
    def addPipelineResult[R <:JsonResult[_]](pr: PipelineResult[R], sinkFunction: SinkFunction[R], props: AppConfig) = pr.addToStream(ds, sinkFunction, props)
    def addPipelineResult[R <:JsonResult[_]](pr: PipelineResult[R], props: AppConfig) = pr.addToStream(ds, props)
  }
}
