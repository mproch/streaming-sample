package pl.mproch.streaming.flink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.util.Collector
import pl.mproch.streaming.flink.FlinkCommonProcess._
import pl.mproch.streaming.model.AverageRate

object FlinkFilterJoin extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  env.addSource(kafkaConsumer[Message]("messages"))
    .connect(env.addSource(kafkaConsumer[User]("users")))
    .keyBy(_.userId, _.userId)
    .flatMap(new RichCoFlatMapFunction[Message, User, MessageWithUser] {

      var state: ValueState[Option[User]] = _

      override def open(parameters: Configuration) = {
        state = getRuntimeContext.getState(new ValueStateDescriptor[Option[User]]("user", classOf[Option[User]], None))
      }

      override def flatMap2(user: User, collector: Collector[MessageWithUser]) = {
        state.update(Some(user))
      }

      override def flatMap1(message: Message, collector: Collector[MessageWithUser]) = {
        collector.collect(message.addUser(state.value()))
      }
    })
    .filter(_.user.exists(_.rank > 10))
    .addSink(kafkaProducer[MessageWithUser]("highRankUsers"))


  env.addSource(kafkaConsumer[Message]("messages"))
    .keyBy(_.text)
    .filterWithState[AverageRate]((mess, maybeState) => {
    val shouldEmit = maybeState.map(_.computeRate() > mess.rate - 1).getOrElse(true)
    val newState = maybeState.getOrElse(new AverageRate()).add(mess.rate)
    (shouldEmit, Some(newState))
  })
    .addSink(kafkaProducer[Message]("lowRankedMessages"))


  env.execute

}
