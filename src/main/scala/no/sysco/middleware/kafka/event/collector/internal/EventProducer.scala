package no.sysco.middleware.kafka.event.collector.internal

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, Props }
import no.sysco.middleware.kafka.event.proto.collector.{ ClusterEvent, CollectorEvent, NodeEvent, TopicEvent }
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

object EventProducer {

  def props(bootstrapServers: String, eventTopic: String): Props = {
    val producerConfigs = new Properties()
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
    val producer: Producer[String, Array[Byte]] = new KafkaProducer(producerConfigs)
    Props(new EventProducer(eventTopic, producer))
  }

  def props(eventTopic: String, producer: Producer[String, Array[Byte]]) =
    Props(new EventProducer(eventTopic, producer))
}
/**
 * Publish Cluster events.
 */
class EventProducer(eventTopic: String, producer: Producer[String, Array[Byte]])
  extends Actor with ActorLogging {

  def handleEvent(event: CollectorEvent): Unit = {
    log.info("Handling collector event {}-{}.", event.entityType, event.entityId)
    val byteArray = event.toByteArray
    producer.send(
      new ProducerRecord(
        eventTopic,
        s"${event.entityType}-${event.entityId}",
        byteArray))
  }

  override def postStop(): Unit = producer.close(1, TimeUnit.SECONDS)

  override def receive(): Receive = {
    case event: CollectorEvent => handleEvent(event)
    case clusterEvent: ClusterEvent =>
      self !
        CollectorEvent(
          CollectorEvent.EntityType.CLUSTER,
          clusterEvent.id,
          CollectorEvent.Value.ClusterEvent(clusterEvent))
    case nodeEvent: NodeEvent =>
      self !
        CollectorEvent(
          CollectorEvent.EntityType.NODE,
          String.valueOf(nodeEvent.id),
          CollectorEvent.Value.NodeEvent(nodeEvent))
    case topicEvent: TopicEvent =>
      self !
        CollectorEvent(
          CollectorEvent.EntityType.TOPIC,
          topicEvent.name,
          CollectorEvent.Value.TopicEvent(topicEvent))
  }
}
