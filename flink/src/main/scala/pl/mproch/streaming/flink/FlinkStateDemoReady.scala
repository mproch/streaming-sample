package pl.mproch.streaming.flink

import org.apache.flink.streaming.api.scala._
import pl.mproch.streaming.flink.FlinkCommonProcess._
import pl.mproch.streaming.model.AverageRate
import pl.mproch.streaming.model.Topics._

object FlinkStateDemoReady extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  env.addSource(kafkaConsumer[Message](messages))
    .map(m => MessagesByUser(m.userId, 1))
    .keyBy(_.userId)
    .sum(1)
    .addSink(kafkaProducer[MessagesByUser](messageCount))


  env.addSource(kafkaConsumer[Message](messages))
    .keyBy(_.topic)
    .filterWithState[AverageRate]((mess, maybeState) => {
    val shouldEmit = maybeState.map(_.computeRate() > mess.rate + 1).getOrElse(true)
    val newState = maybeState.getOrElse(new AverageRate()).add(mess.rate)
    (shouldEmit, Some(newState))
  })
    .addSink(kafkaProducer[Message](lowRankMessages))

  env.execute("flink1")
}
