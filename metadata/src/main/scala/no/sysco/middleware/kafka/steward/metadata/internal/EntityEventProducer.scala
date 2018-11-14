package no.sysco.middleware.kafka.steward.metadata.internal

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, Props }
import no.sysco.middleware.kafka.steward.metadata.internal.Parser._
import no.sysco.middleware.kafka.steward.metadata.model.{ EntityCreated, EntityUpdated }
import no.sysco.middleware.kafka.steward.proto.metadata.MetadataEvent
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

object EntityEventProducer {
  def props(bootstrapServers: String, eventTopic: String): Props = {
    val producerConfigs = new Properties()
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
    val producer: Producer[String, Array[Byte]] = new KafkaProducer(producerConfigs)
    Props(new EntityEventProducer(eventTopic, producer))
  }

  def props(eventTopic: String, producer: Producer[String, Array[Byte]]) =
    Props(new EntityEventProducer(eventTopic, producer))
}

class EntityEventProducer(eventTopic: String, producer: Producer[String, Array[Byte]]) extends Actor with ActorLogging {
  override def receive: Receive = {
    case entityCreated: EntityCreated => handleMetadataEvent(toPb(entityCreated))
    case entityUpdated: EntityUpdated => handleMetadataEvent(toPb(entityUpdated))
  }

  def handleMetadataEvent(event: MetadataEvent): Unit = {
    log.info("Handling collector event {}-{}.", event.entity.get.`type`, event.entity.get.id)
    val byteArray = event.toByteArray
    val record = new ProducerRecord(eventTopic, s"${event.entity.get.id}-${event.entity.get.id}", byteArray)
    record.headers().add("entity_type", event.entity.get.`type`.name.getBytes)
    producer.send(record)
  }

  override def postStop(): Unit = producer.close(1, TimeUnit.SECONDS)
}
