package pl.mproch.streaming.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer09, FlinkKafkaConsumer09}
import FlinkCommonProcess._
import org.apache.flink.util.Collector

object FlinkWindowReduce extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(5)

  env.setParallelism(10)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val messages = env.addSource(new FlinkKafkaConsumer09[Message]("messages", schema[Message], prepareKafkaProperties))
      .assignAscendingTimestamps(_.time)

  messages
    .map(m => MessagesByUser(m.userId, 1))
    .keyBy(_.userId)
    .sum(1)
    .addSink(new FlinkKafkaProducer09[MessagesByUser]("messageCount", schema[MessagesByUser], prepareKafkaProperties))

  messages
      .map(m => MessagesByUser(m.userId, 1))
      .keyBy(_.userId)
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
        .apply[TimedMessagesByUser]((k:String, w:TimeWindow, mes:Iterable[MessagesByUser], c:Collector[TimedMessagesByUser])
          => c.collect(TimedMessagesByUser(k, mes.size, w.getStart)))
        .addSink(new FlinkKafkaProducer09[TimedMessagesByUser]("timedMessageCount", schema[TimedMessagesByUser], prepareKafkaProperties))

  env.execute("FlinkJob1")


  case class MessagesByUser(userId: String, count: Int)
  case class TimedMessagesByUser(userId: String, count: Int, startTime: Long)

}
