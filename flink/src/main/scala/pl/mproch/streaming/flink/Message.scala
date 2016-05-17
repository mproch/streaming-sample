package pl.mproch.streaming.flink

case class Message(userId: String, topic: String, rate: Int, time: Long) {
  def withUser(user: Option[User]) = MessageWithUser(user, topic, rate, time)
}

case class User(userId: String, rank: Int, age: Int)

case class MessageWithUser(user: Option[User], topic: String, rate: Int, time: Long)


case class MessagesByUser(userId: String, count: Int)

case class TimedMessagesByUser(userId: String, count: Int, startTime: Long)