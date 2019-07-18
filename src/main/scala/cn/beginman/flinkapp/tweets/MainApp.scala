package cn.beginman.flinkapp.tweets

import cn.beginman.flinkapp.tweets.analytics.{AggResult, HashtagResult}
import org.apache.flink.streaming.api.scala._
import cn.beginman.flinkapp.tweets.streaming.StreamFactory
import cn.beginman.flinkapp.tweets.utils.ElasticUtils
import cn.beginman.flinkapp.tweets.analytics.DataStreamImplicits._
import cn.beginman.flinkapp.tweets.config.AppConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object MainApp {

  lazy val log: Logger = LoggerFactory.getLogger(MainApp.getClass)

  def main(args: Array[String]): Unit = {
    log.info("Starting Tweets App ...")

    // 加载配置文件
    val porpsOpt = AppConfig.getProps(args)
    if (porpsOpt.isEmpty) {
      Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("help.txt"))
        .getLines()
        .foreach(println)
      System.exit(0)
    }

    val props = porpsOpt.get

    // check es集群是否可用
    new ElasticUtils(props.elasticUrl).ensureElasticSearchCluster()

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 5000))

    // create stream
    val stream = StreamFactory.createStream(env, props)
    // debug
    // stream.print()

    // add results to stream
    Seq(HashtagResult,
      AggResult
    ).foreach(r => {
      log.info(s"Adding result to stream: ${r.name()} -- ${r.description()}")
      stream.addPipelineResult(r, props)
    })
    env.execute("tweet")
  }

}
