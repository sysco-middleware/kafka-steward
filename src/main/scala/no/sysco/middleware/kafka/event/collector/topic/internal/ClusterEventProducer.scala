package no.sysco.middleware.kafka.event.collector.topic.internal

import java.util.Properties

import akka.actor.{ Actor, Props }
import no.sysco.middleware.kafka.event.proto.collector.TopicEvent
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

object ClusterEventProducer {

  def props(bootstrapServers: String, topicEventTopic: String): Props = {
    val producerConfigs = new Properties()
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
    val producer: Producer[String, Array[Byte]] = new KafkaProducer(producerConfigs)
    Props(new ClusterEventProducer(topicEventTopic, producer))
  }

  def props(topicEventTopic: String, producer: Producer[String, Array[Byte]]) =
    Props(new ClusterEventProducer(topicEventTopic, producer))
}

/**
 * Publish Topic events.
 */
class ClusterEventProducer(topicEventTopic: String, producer: Producer[String, Array[Byte]]) extends Actor {

  def handleTopicEvent(topicEvent: TopicEvent): Unit = {
    val byteArray = topicEvent.toByteArray
    producer.send(new ProducerRecord(topicEventTopic, topicEvent.name, byteArray)).get()
  }

  override def receive: Receive = {
    case topicEvent: TopicEvent => handleTopicEvent(topicEvent)
  }

  override def postStop(): Unit = producer.close()
}
