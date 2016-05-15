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

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(5)

  env.setParallelism(10)

  env.addSource(new FlinkKafkaConsumer09[Message]("messages", schema[Message], prepareKafkaProperties))
    .connect(env.addSource(new FlinkKafkaConsumer09[User]("users", schema[User], prepareKafkaProperties)))
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
    .addSink(
      new FlinkKafkaProducer09[MessageWithUser]("highRankUsers", schema[MessageWithUser], prepareKafkaProperties))


  env.addSource(new FlinkKafkaConsumer09[Message]("messages", schema[Message], prepareKafkaProperties))
    .keyBy(_.text)
    .filterWithState[AverageRate]((mess, maybeState) => {
    val shouldEmit = maybeState.map(_.computeRate() > mess.rate - 1).getOrElse(true)
    val newState = maybeState.getOrElse(new AverageRate()).add(mess.rate)
    (shouldEmit, Some(newState))
  })
    .addSink(
      new FlinkKafkaProducer09[Message]("lowRankedMessages", schema[Message], prepareKafkaProperties))


  env.execute

}
