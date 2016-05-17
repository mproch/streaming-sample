package pl.mproch.streaming.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import pl.mproch.streaming.flink.FlinkCommonProcess._
import pl.mproch.streaming.model.Topics._

object FlinkWindowDemoReady extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val messagesSource = env.addSource(kafkaConsumer[Message](messages))
    .assignAscendingTimestamps(_.time)


  messagesSource
    .keyBy(_.topic)
    .timeWindow(Time.seconds(10))
    .fold[List[String]](List(), (ac: List[String], mess: Message) => mess.userId :: ac)
    .map(_.toString())
    .addSink(kafkaProducer[String](usersPostingMessage))

  messagesSource
    .map(m => MessagesByUser(m.userId, 1))
    .keyBy(_.userId)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
    .apply[TimedMessagesByUser](
    (k: String, w: TimeWindow, mes: Iterable[MessagesByUser], c: Collector[TimedMessagesByUser])
    => c.collect(TimedMessagesByUser(k, mes.size, w.getStart)))
    .addSink(kafkaProducer[TimedMessagesByUser](timedMessageCount))

  env.execute("FlinkJob1")


}
