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

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 根据其内部时间戳为Sensors分配时间戳，并在5秒钟内发出水印。
  * 5s 为在记录中看到的最大可见时间戳与要发出的水印的时间戳之间的（固定）间隔。
  */
class SensorTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(5)) {

  /** 从 Sensor 抽取时间戳, flink调用该方法为数据元分配时间戳 */
  override def extractTimestamp(r: Sensor): Long = r.timestamp

}
