package no.sysco.middleware.kafka.event.collector.cluster.internal

import java.util.Properties

import akka.actor.{ Actor, Props }
import no.sysco.middleware.kafka.event.proto.collector.ClusterEvent
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

object ClusterEventProducer {
  def props(bootstrapServers: String, clusterEventTopic: String): Props = {
    val producerConfigs = new Properties()
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
    val producer: Producer[String, Array[Byte]] = new KafkaProducer(producerConfigs)
    Props(new ClusterEventProducer(clusterEventTopic, producer))
  }

  def props(clusterEventTopic: String, producer: Producer[String, Array[Byte]]) =
    Props(new ClusterEventProducer(clusterEventTopic, producer))

}

/**
 * Publish Cluster events.
 */
class ClusterEventProducer(clusterEventTopic: String, producer: Producer[String, Array[Byte]]) extends Actor {

  def handleClusterEvent(clusterEvent: ClusterEvent): Unit = {
    val byteArray = clusterEvent.toByteArray
    producer.send(new ProducerRecord(clusterEventTopic, clusterEvent.id, byteArray)).get()
  }

  override def receive: Receive = {
    case clusterEvent: ClusterEvent => handleClusterEvent(clusterEvent)
  }

  override def postStop(): Unit = producer.close()
}
