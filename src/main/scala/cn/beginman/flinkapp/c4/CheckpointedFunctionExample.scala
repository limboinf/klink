package cn.beginman.flinkapp.c4

import cn.beginman.flinkapp.util.{Sensor, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


object CheckpointedFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    // 设置一致性级别为exactly-once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置检查点超时时间为60000ms, 如果超时则丢弃这个检查点
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 设置快照失败后任务继续正常执行
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置并发检查点数量为2
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[Sensor] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val highTempCnts = sensorData
      .keyBy(_.id)
      .flatMap(new HighTempCounter(10.0))

    highTempCnts.print()
    env.execute()
  }
}

class HighTempCounter(val threshold: Double)
  extends FlatMapFunction[Sensor, (String, Long, Long)] with CheckpointedFunction {

  // 高温计数
  var opHighTempCnt: Long = 0

  var keyedCntState: ValueState[Long] = _
  var opCntState: ListState[Long] = _

  override def flatMap(v: Sensor, out: Collector[(String, Long, Long)]): Unit = {
    if (v.temperature > threshold) {
      opHighTempCnt += 1
      // update keyed high temp counter
      val keyHighTempCnt = keyedCntState.value() + 1
      keyedCntState.update(keyHighTempCnt)

      // emit new counters
      out.collect((v.id, keyHighTempCnt, opHighTempCnt))
    }
  }

  override def initializeState(initContext: FunctionInitializationContext): Unit = {
    // initialize keyed state
    val keyCntDescriptor = new ValueStateDescriptor[Long]("keyedCnt", classOf[Long])
    keyedCntState = initContext.getKeyedStateStore.getState(keyCntDescriptor)

    // initialize operator state
    val opCntDescriptor = new ListStateDescriptor[Long]("opCnt", classOf[Long])
    opCntState = initContext.getOperatorStateStore.getListState(opCntDescriptor)
    // initialize local variable with state
    opHighTempCnt = opCntState.get().asScala.sum
  }

  override def snapshotState(snapshotContext: FunctionSnapshotContext): Unit = {
    // update operator state with local state
    opCntState.clear()
    opCntState.add(opHighTempCnt)
  }
}

