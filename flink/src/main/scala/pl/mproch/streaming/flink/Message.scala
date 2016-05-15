package pl.mproch.streaming.flink

case class Message(userId: String, text: String, rate: Int, time: Long) {
  def addUser(user: Option[User]) = MessageWithUser(user, text, rate, time)
}

case class User(userId: String, name: String, rank: Int, age: Int)

case class MessageWithUser(user: Option[User], text: String, rate: Int, time: Long)
