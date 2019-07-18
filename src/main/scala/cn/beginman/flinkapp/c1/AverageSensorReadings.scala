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
package cn.beginman.flinkapp.c1

import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AverageSensors {

  def main(args: Array[String]) {

    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用EventTime作为基础时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval, 1s
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[Sensor] = env
      // 添加自定义source, 随机生成数据
      .addSource(new SensorSource)
      // 分配事件时间所需的时间戳和水印
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[Sensor] = sensorData
      .map( r =>
      Sensor(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      .keyBy(_.id)
      .timeWindow(Time.seconds(1))
      // apply 用户自定义函数来计算平均值
      .apply(new TemperatureAverager)

    avgTemp.print()

    // execute application
    env.execute("Compute average sensor temperature")
  }
}

/** 用户自定义窗口函数(UDWF, User-defined WindowFunction) 计算平台温度 */
class TemperatureAverager extends WindowFunction[Sensor, Sensor, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
    sensorId: String,
    window: TimeWindow,
    vals: Iterable[Sensor],
    out: Collector[Sensor]): Unit = {

    val (cnt, sum) = vals.foldLeft((0, 0.0))( (c, r) => (c._1 + 1, c._2 + r.temperature) )
    val avgTemp = sum / cnt

    out.collect(Sensor(sensorId, window.getEnd, avgTemp))
  }
}
