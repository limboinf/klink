/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.beginman.flinkapp.util

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
  * 自定义source，随机生成Sensor值
  *
  * source的每个并行实例模拟10个传感器，每100毫秒发射一个传感器读数。
  *
  * 注意：这只是一个简单示例，没有 checkpoint state, 如果失败也没有任何反馈
  */
class SensorSource extends RichParallelSourceFunction[Sensor] {

  var running: Boolean = true

  val RND_MAX = 100
  // val RND_MAX = 10

  /**
    * 实现SourceFunction的 run方法，签名：
    * void run(SourceContext<T> ctx) throws Exception;
    *
    * SourceContext 为源函数用于发出元素和可能的水印的接口。
    * @param srcCtx
    */
  override def run(srcCtx: SourceContext[Sensor]): Unit = {

    val rand = new Random()
    // 查找此并行任务的索引
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    // 初始化数据
    var curFTemp = (1 to RND_MAX).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    // 发出数据直到被取消
    while (running) {

      // update temperature
      curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // 发射数据
      curFTemp.foreach( t => srcCtx.collect(Sensor(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

}
