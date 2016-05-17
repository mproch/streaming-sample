package pl.mproch.streaming.flink

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, KeyedSerializationSchema, SerializationSchema}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import pl.mproch.streaming.model.Topics


object FlinkCommonProcess {

  def kafkaProducer[T <: AnyRef : Manifest](topic: Topics) = {
    new FlinkKafkaProducer09[T](topic.name(), schema[T], prepareKafkaProperties)
  }


  def kafkaConsumer[T <: AnyRef : Manifest](topic: Topics) = {
    new FlinkKafkaConsumer09[T](topic.name(), schema[T], prepareKafkaProperties)
  }

  def prepareKafkaProperties = {
    val props = new Properties()
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "jeeConf-flink")
    props.setProperty("auto.offset.reset", "latest")
    props
  }

  def schema[T <: AnyRef : Manifest] = {

    implicit def formats = Serialization.formats(NoTypeHints)

    val klass = manifest[T].runtimeClass.asInstanceOf[Class[T]]

    new SerializationSchema[T] with DeserializationSchema[T] {

      override def serialize(t: T) = write[T](t).getBytes

      override def isEndOfStream(t: T) = false

      override def deserialize(bytes: Array[Byte]) = read[T](new String(bytes))

      override def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(klass)

    }
  }

  def keyedSchema[T <: AnyRef : Manifest](key: T => String) = new KeyedSerializationSchema[T] {

    implicit def formats = Serialization.formats(NoTypeHints)

    override def serializeValue(t: T) = write[T](t).getBytes

    override def serializeKey(t: T) = key(t).getBytes
  }


}
