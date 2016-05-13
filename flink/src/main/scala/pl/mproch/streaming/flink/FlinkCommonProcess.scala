package pl.mproch.streaming.flink

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

trait FlinkCommonProcess {

  protected def prepareKafkaProperties = {
    val props = new Properties()
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "jeeConf-flin")
    props.setProperty("auto.offset.reset", "earliest")
    props
  }

  protected def schema[T:Manifest] = {
    val om = new ObjectMapper()

    val klass = manifest[T].runtimeClass.asInstanceOf[Class[T]]

    new SerializationSchema[T] with DeserializationSchema[T] {

      override def serialize(t: T) = om.writeValueAsBytes(t)

      override def isEndOfStream(t: T) = false

      override def deserialize(bytes: Array[Byte]) = om.readValue(bytes, klass)

      override def getProducedType : TypeInformation[T] = TypeExtractor.getForClass(klass)


    }
  }


}
