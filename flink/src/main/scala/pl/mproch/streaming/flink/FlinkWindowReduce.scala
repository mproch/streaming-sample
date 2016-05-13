package pl.mproch.streaming.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer09, FlinkKafkaConsumer09}

object FlinkWindowReduce extends App with FlinkCommonProcess {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(5)

  env.setParallelism(10)

  env.addSource(new FlinkKafkaConsumer09[Message]("messages", schema[Message], prepareKafkaProperties))
    .filter(_.rate > 5)
    .keyBy(_.userId)
        .addSink(new FlinkKafkaProducer09[Message]("flinkMessage", schema[Message], prepareKafkaProperties))

  env.execute("FlinkJob1")
}
