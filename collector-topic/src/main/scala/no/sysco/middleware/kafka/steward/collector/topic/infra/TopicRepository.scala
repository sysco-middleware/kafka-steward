package no.sysco.middleware.kafka.steward.collector.topic.infra

import akka.actor.{ Actor, ActorLogging, Props }
import no.sysco.middleware.kafka.steward.collector.proto
import no.sysco.middleware.kafka.steward.collector.proto.topic._
import no.sysco.middleware.kafka.steward.collector.topic.core.model.{ TopicCreated, TopicDeleted, TopicUpdated }
import org.apache.kafka.clients.producer.{ Callback, Producer, ProducerRecord }

object TopicRepository {
  def props(producer: Producer[Array[Byte], Array[Byte]], topic: String): Props =
    Props(new TopicRepository(producer, topic))
}

class TopicRepository(producer: Producer[Array[Byte], Array[Byte]], topic: String) extends Actor with ActorLogging {
  override def receive: Receive = {
    case topicCreated: TopicCreated => handleTopicCreated(topicCreated)
    case topicUpdated: TopicUpdated => handleTopicUpdated(topicUpdated)
    case topicDeleted: TopicDeleted => handleTopicDeleted(topicDeleted)
  }

  private def handleTopicCreated(topicCreated: TopicCreated): Unit =
    send(
      new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        TopicKey(topicCreated.clusterId.id, topicCreated.name).toByteArray,
        TopicValue(TopicValue.Event.TopicCreated(proto.topic.TopicCreated())).toByteArray))

  private def handleTopicUpdated(topicUpdated: TopicUpdated): Unit =
    send(
      new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        TopicKey(topicUpdated.clusterId.id, topicUpdated.topic.name).toByteArray,
        TopicValue(
          TopicValue.Event.TopicUpdated(
            proto.topic.TopicUpdated(
              topicUpdated.topic.partitions.map { p =>
                Partition(
                  p.id,
                  p.replicas.map { r => Replica(r.brokerId.id) }.toSeq)
              }.toSeq,
              Some(Config(topicUpdated.topic.config.entries))))).toByteArray))

  private def handleTopicDeleted(topicDeleted: TopicDeleted): Unit =
    send(
      new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        TopicKey(topicDeleted.clusterId.id, topicDeleted.name).toByteArray,
        TopicValue(TopicValue.Event.TopicDeleted(proto.topic.TopicDeleted())).toByteArray))

  val callback: Callback = (metadata, error) => {
    Option(error) match {
      case None => log.info("Topic event produced: {}", metadata)
      case Some(_) => log.error(error, "Error storing Topic event")
    }
  }

  private def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = producer.send(record, callback)
}
