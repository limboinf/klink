package cn.beginman.flinkapp.util

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class ProductEventSource extends RichParallelSourceFunction[UserBehavior] {

  var running: Boolean = true

  override def run(ctx: SourceContext[UserBehavior]): Unit = {

    val rand = new Random()
    val behaviors = List("pv", "buy", "cart", "fav")

    while (running) {
      val userId: Long =  rand.nextInt(100).toLong
      val itemId: String = "item_" + rand.nextInt(20)
      val categoryId: Int = rand.nextInt(100)
      val behavior:String = behaviors(rand.nextInt(behaviors.size))
      val timestamp = Calendar.getInstance.getTimeInMillis

      ctx.collect(UserBehavior(userId, itemId, categoryId, behavior, timestamp))

      Thread.sleep(50)
   }
  }

  override def cancel(): Unit = {
    running = false
  }
}
