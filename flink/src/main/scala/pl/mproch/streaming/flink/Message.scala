package pl.mproch.streaming.flink

case class Message(userId: String, text: String, rate: Int, timestamp: Long)

case class User(userId: String, name: String, rank: Int, age: Int)
