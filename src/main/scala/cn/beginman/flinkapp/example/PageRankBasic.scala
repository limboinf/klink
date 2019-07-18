package cn.beginman.flinkapp.example

import java.lang.Iterable
import scala.collection.JavaConverters._

import cn.beginman.flinkapp.utils.PageRankData
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.java.aggregation.Aggregations.SUM
import org.apache.flink.util.Collector
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object PageRankBasic {

  private final val DAMPENING_FACTOR: Double = 0.85
  private final val EPSILON: Double = 0.0001

  // *************************************************************************
  //     USER TYPES
  // *************************************************************************
  case class Link(sourceId: Long, targetId: Long)
  case class Page(pageId: Long, rank: Double)
  case class AdjacencyList(sourceId: Long, targetIds: Array[Long])

  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 使参数在webUI可见
    env.getConfig.setGlobalJobParameters(params)
    // 读取数据
    val (pages, numPages) = getPagesDataSet(env, params)
    val links = getLinksDataSet(env, params)
    val maxIterations = params.getInt("iterations", 10)

    // 为页面分配初始排名
    val pagesWithRanks = pages.map {p=>Page(p, 1.0/numPages)}.withForwardedFields("*->pageId")

    // 从链接输入构建邻接列表
    val adjacencyLists = links
      .groupBy("sourceId")
      .reduceGroup(new GroupReduceFunction[Link, AdjacencyList] {
        override def reduce(values: Iterable[Link], out: Collector[AdjacencyList]) = {
          var outputId = 1L
          val outputList = values.asScala map { t => outputId = t.sourceId; t.targetId }
          out.collect(new AdjacencyList(outputId, outputList.toArray))
        }
      })

    // start iteration
    val finalRanks = pagesWithRanks.iterateWithTermination(maxIterations) {
      currentRanks =>
        val newRanks = currentRanks
        // 将排名分配给目标页面
          .join(adjacencyLists).where("pageId").equalTo("sourceId") {
          (page, adjacent, out:Collector[Page]) =>
            val targets = adjacent.targetIds
            val len = targets.length
            adjacent.targetIds.foreach{ t => out.collect(Page(t, page.rank / len)) }
        }
          // 收集排名并求和
          .groupBy("pageId").aggregate(SUM, "rank")
        // 应用阻尼因子
          .map { p =>
          Page(p.pageId, (p.rank * DAMPENING_FACTOR) + ((1 - DAMPENING_FACTOR) / numPages))
        }.withForwardedFields("pageId")

        // 如果没有排名更新，则终止
        val termination = currentRanks.join(newRanks).where("pageId").equalTo("pageId") {
          (current, next, out: Collector[Int]) =>
            // 检查重大更新
            if (math.abs(current.rank - next.rank) > EPSILON) out.collect(1)
        }
        (newRanks, termination)
    }

    val result = finalRanks

    // emit result
    if (params.has("output")) {
      result.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Basic PageRank Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      result.print()
    }

  }

  /**
    * 从csv文件获取page DataSet
    * @param env ExecutionEnvironment
    * @param params ParameterTool
    * @return Tuple (DataSet[Long], Long)
    */
  def getPagesDataSet(env:ExecutionEnvironment, params: ParameterTool) :(DataSet[Long], Long) = {
    if (params.has("pages") && params.has("numPages")) {
      val pages = env
        .readCsvFile[Tuple1[Long]](params.get("pages"), fieldDelimiter = " ", lineDelimiter = "\n")
        .map(x => x._1)
      (pages, params.getLong("numPages"))
    } else {
      println("Executing PageRank example with default pages data set.")
      println("Use --pages and --numPages to specify file input.")
      (env.generateSequence(1, 15), PageRankData.getNumberOfPages)
    }
  }

  def getLinksDataSet(env: ExecutionEnvironment, params: ParameterTool): DataSet[Link] = {
    if (params.has("links")) {
      env.readCsvFile[Link](params.get("links"), fieldDelimiter = " ", includedFields = Array(0, 1))
    } else {
      println("Executing PageRank example with default links data set.")
      println("Use --links to specify file input.")
      val edges = PageRankData.EDGES.map { case Array(v1, v2) => Link(v1.asInstanceOf[Long], v2.asInstanceOf[Long])}
      env.fromCollection(edges)
    }
  }

}
