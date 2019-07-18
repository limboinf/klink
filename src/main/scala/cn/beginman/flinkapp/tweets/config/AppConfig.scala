package cn.beginman.flinkapp.tweets.config

import java.util.Properties

import cn.beginman.flinkapp.tweets.config.EventStreamSource.EventStreamSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}

object EventStreamSource extends Enumeration {
  type EventStreamSource = Value
  val Kafka, File = Value
}


case class AppConfig(source: EventStreamSource,
                     parallelism:Int = 1,
                     windowTime:Time,
                     kafkaServers:String="",
                     kafkaTopic:String="",
                     kafkaGroupId: String="flink-app",
                     elasticUrl:String=""
                    )


/**
  * 获取配置
  * 从--configFile 配置文件获取，或者从参数里解析
  */
object AppConfig {

  lazy val log: Logger = LoggerFactory.getLogger(AppConfig.getClass)

  def getProps(args: Array[String]) : Option[AppConfig] = {

    val params:ParameterTool = ParameterTool.fromArgs(args)

    // 从配置文件里解析
    val props = params.has("configFile") match {
      case true => ParameterTool.fromPropertiesFile(params.get("configFile")).getProperties
      case _ => new Properties
    }
    // 覆盖配置参数
    props.putAll(params.getProperties)

    if (props.size > 0) {
      // 打印配置
      props.stringPropertyNames().asScala.foreach(p => {
        // 密码脱敏, 不在日志里打印出来
        val value = if (p.toLowerCase.contains("secret")) "xxxxxxx" else props.getProperty(p)
        log.info(s"配置参数：$p = $value")
      })

      val streamSource: EventStreamSource = EventStreamSource.Kafka

      props.stringPropertyNames().asScala.foreach(p => {
        props.getProperty(p) match {
          case "kafka" => streamSource
          case _ => streamSource
        }
      })

      // create props obj
      Some(AppConfig(
        streamSource,
        props.getProperty("parallelism", "1").toInt,
        parseWindowTime(props.getProperty("timeWindow", "60s")),
        props.getProperty("kafkaServers", ""),
        props.getProperty("kafkaTopic", "tweets"),
        props.getProperty("kafkaGroupId", "flink-app"),
        props.getProperty("elasticUrl", "http://localhost:9200")
      ))
    } else {
      None
    }

  }

  // 解析窗口参数
  def parseWindowTime(time: String): Time = {
    val timeRegex = "([0-9]+)([sm]*)".r
    time.trim match {
      case timeRegex(t, sm) => {
        sm match {
          case "m" => Time.minutes(t.toLong)
          case "s" => Time.seconds(t.toLong)
        }
      }
    }
  }
}