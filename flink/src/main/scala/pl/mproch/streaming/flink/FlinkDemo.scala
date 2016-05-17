package pl.mproch.streaming.flink

import org.apache.flink.streaming.api.scala._
import pl.mproch.streaming.flink.FlinkCommonProcess._
import pl.mproch.streaming.model.Topics._

object FlinkDemo extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  env
    .addSource(kafkaConsumer[Message](messages))


  env.execute("flinkDemo")
}
