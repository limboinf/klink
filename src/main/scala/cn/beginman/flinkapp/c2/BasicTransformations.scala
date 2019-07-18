/**
  * 基础算子操作
  * UDF：
  *   - MapFunction
  *   - FilterFunction
  *   - FlatMapFunction
  *   - ...
  */
package cn.beginman.flinkapp.c2

import org.apache.flink.api.scala._
import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


object BasicTransformations {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

//    val filteredSensors: DataStream[Sensor] = readings
//      .filter( r =>  r.temperature >= 25)

   // the above filter transformation using a UDF
    val filteredSensors: DataStream[Sensor] = readings
      .filter(new TemperatureFilter(25))

    val sensorIds: DataStream[String] = filteredSensors
      .map( r => r.id )

    // the above map transformation using a UDF
    // val sensorIds2: DataStream[String] = readings
    //   .map(new ProjectionMap)

    // split the String id of each sensor to the prefix "sensor" and sensor number
    val splitIds: DataStream[String] = sensorIds
      .flatMap( id => id.split("_") )

    // the above flatMap transformation using a UDF
    // val splitIds: DataStream[String] = sensorIds
    //  .flatMap( new SplitIdFlatMap )

    // print result stream to standard out
    splitIds.print()

    // execute application
    env.execute("Basic Transformations Example")
  }

  /** 用户自定义 FilterFunction 函数(UDF) */
  class TemperatureFilter(threshold: Long) extends FilterFunction[Sensor] {

    override def filter(r: Sensor): Boolean = r.temperature >= threshold

  }

  /** 用户自定义 MapFunction 函数 */
  class ProjectionMap extends MapFunction[Sensor, String] {

    override def map(r: Sensor): String  = r.id

  }

  /** 用户自定义 FlatMapFunction 函数 */
  class SplitIdFlatMap extends FlatMapFunction[String, String] {

    override def flatMap(id: String, collector: Collector[String]): Unit = id.split("_")

  }

}
