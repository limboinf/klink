package cn.beginman.flinkapp.tweets.utils

import java.net.URL

import akka.japi.Option.Some
import com.twitter.finagle.Http
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.RequestBuilder
import com.twitter.util.{Await, Duration}
import net.liftweb.json.{NoTypeHints, Serialization}
import org.slf4j.{Logger, LoggerFactory}
import net.liftweb.json._
import cn.beginman.flinkapp.tweets.analytics.JsonResult
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.JavaConverters

// es health
// curl localhost:9200/_cluster/health 返回结果
case class ElasticClusterHealth(cluster_name: String,
                                status : String,
                                timed_out : Boolean,
                                number_of_nodes: Int,
                                number_of_data_nodes : Int,
                                active_primary_shards : Int,
                                active_shards : Int,
                                relocating_shards : Int,
                                initializing_shards : Int,
                                unassigned_shards : Int,
                                delayed_unassigned_shards: Int,
                                number_of_pending_tasks : Int,
                                number_of_in_flight_fetch: Int,
                                task_max_waiting_in_queue_millis: Int,
                                active_shards_percent_as_number: Float)

class ElasticUtils(path: String) {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val url = new URL(path)
  lazy val client = ClientBuilder()
    .hosts(path.replace(url.getProtocol + "://", ""))
    .hostConnectionLimit(10)
    .tcpConnectTimeout(Duration.fromSeconds(10))
    .retries(2)
    .stack(if (url.getProtocol == "https") Http.client.withTls(url.getHost) else Http.client)
    .build()

  implicit val formats = Serialization.formats(NoTypeHints)


  def isClusterUp(): Boolean = {
    val health = getClusterHealth()
    if (health.isDefined) {
      log.info(s"Es cluster status: ${health.get.status}")
      health.get.status.equals("yellow") || health.get.status.equals("green")
    } else {
      false
    }
  }

  def getClusterHealth(): Option[ElasticClusterHealth] = {
    val healthResponse = Await.result(client(RequestBuilder.create().url(path + "/_cluster/health").buildGet()))
    if (healthResponse.statusCode == 200) {
      // need import net.liftweb.json._
      Some(parse(healthResponse.getContentString()).extract[ElasticClusterHealth])
    } else {
      None
    }
  }

  def ensureElasticSearchCluster() = {
    isClusterUp() match {
      case true => log.info("Es cluster normal")
      case false => throw new Exception(s"Es cluster not avaliable at $path")
    }
  }

}


object ElasticUtils {

  val indices = Seq("event", "ping")

  /**
    * ES sink
    * @param idx 索引名称
    * @param tp  _type 名称
    * @param elasticUrl es地址
    * @tparam T 数据类
    * @return
    */
  def createSink[T <: JsonResult[_]](idx: String, tp: String, elasticUrl: String): ElasticsearchSink[T] = {
    val httpHosts = elasticUrl.split(",").map(HttpHost.create(_)).toSeq
    // 插入数据
    val sinkFunc = new ElasticsearchSinkFunction[T] {
      override def process(t: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // println(s"json序列化：${t.toJson()}")
        requestIndexer.add(Requests.indexRequest()
            .index(idx)
            .`type`(tp)
            .source(t.toJson(), XContentType.JSON)
        )
      }
    }

    import collection.JavaConverters._
    val esSinkBuilder = new ElasticsearchSink.Builder[T](httpHosts.asJava, sinkFunc)
    // 设置要为每个批量请求缓冲的最大操作数。
    esSinkBuilder.setBulkFlushMaxActions(1000)
    // 设置批量刷新间隔，以毫秒为单位。
    esSinkBuilder.setBulkFlushInterval(30000)
    // 设置插入重试次数
    esSinkBuilder.setBulkFlushBackoffRetries(5)
    // 设置重试延迟时间，以毫秒为单位
    esSinkBuilder.setBulkFlushBackoffDelay(500)
    // 自定义rest客户端
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        // TODO Additional rest client args go here - authentication headers for secure connections etc...
      }
    })
    esSinkBuilder.build()
  }
}