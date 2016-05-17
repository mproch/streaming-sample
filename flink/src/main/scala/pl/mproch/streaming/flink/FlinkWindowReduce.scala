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

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val messages = env.addSource(kafkaConsumer[Message]("messages"))
      .assignAscendingTimestamps(_.time)

  messages
    .map(m => MessagesByUser(m.userId, 1))
    .keyBy(_.userId)
    .sum(1)
    .addSink(kafkaProducer[MessagesByUser]("messageCount"))


  messages
        .keyBy(_.text)
          .timeWindow(Time.seconds(10))
            .fold[List[String]](List(), (ac:List[String], mess:Message) => mess.userId::ac)
              .map(_.toString())
                .addSink(kafkaProducer[String]("usersPostingMessage"))

  messages
      .map(m => MessagesByUser(m.userId, 1))
      .keyBy(_.userId)
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
        .apply[TimedMessagesByUser]((k:String, w:TimeWindow, mes:Iterable[MessagesByUser], c:Collector[TimedMessagesByUser])
          => c.collect(TimedMessagesByUser(k, mes.size, w.getStart)))
        .addSink(kafkaProducer[TimedMessagesByUser]("timedMessageCount"))

  env.execute("FlinkJob1")


  case class MessagesByUser(userId: String, count: Int)
  case class TimedMessagesByUser(userId: String, count: Int, startTime: Long)

}
