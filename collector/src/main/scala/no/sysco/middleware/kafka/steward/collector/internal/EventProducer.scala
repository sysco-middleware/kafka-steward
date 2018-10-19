package no.sysco.middleware.kafka.steward.collector.internal

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, Props }
import no.sysco.middleware.kafka.steward.proto.collector.{ BrokerEvent, ClusterEvent, CollectorEvent, TopicEvent }
import no.sysco.middleware.kafka.steward.proto.entity.Entity
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
    log.info("Handling collector event {}-{}.", event.entity.get.`type`, event.entity.get.id)
    val byteArray = event.toByteArray
    val record = new ProducerRecord(eventTopic, s"${event.entity.get.id}-${event.entity.get.id}", byteArray)
    record.headers().add("entity_type", event.entity.get.`type`.name.getBytes)
    producer.send(record)
  }

  override def postStop(): Unit = producer.close(1, TimeUnit.SECONDS)

  override def receive(): Receive = {
    case event: CollectorEvent => handleEvent(event)
    case clusterEvent: ClusterEvent =>
      self !
        CollectorEvent(
          Some(
            Entity(
              Entity.EntityType.CLUSTER,
              clusterEvent.id)),
          CollectorEvent.Value.ClusterEvent(clusterEvent))
    case brokerEvent: BrokerEvent =>
      self !
        CollectorEvent(
          Some(
            Entity(
              Entity.EntityType.BROKER,
              brokerEvent.id)),
          CollectorEvent.Value.BrokerEvent(brokerEvent))
    case topicEvent: TopicEvent =>
      self !
        CollectorEvent(
          Some(
            Entity(
              Entity.EntityType.TOPIC,
              topicEvent.name)),
          CollectorEvent.Value.TopicEvent(topicEvent))
  }
}
