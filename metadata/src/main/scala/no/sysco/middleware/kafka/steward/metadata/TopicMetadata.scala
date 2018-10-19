package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, Props}
import no.sysco.middleware.kafka.steward.metadata.model.EntityStatus.EntityStatus
import no.sysco.middleware.kafka.steward.metadata.model.{EntityStatus, Metadata, Topic, TopicPartition, TopicPartitionReplica}
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent.Value.TopicEvent

object TopicMetadata {
  def props(): Props = Props(new TopicMetadata)
}

class TopicMetadata extends Actor with ActorLogging {

  var topic: Topic = _
  var partitions: List[TopicPartition] = List()
  var metadata: Metadata = Metadata()
  var status: EntityStatus = EntityStatus.Current

  override def receive: Receive = ???

  private def handleTopicEvent(topicEvent: TopicEvent): Unit = topicEvent match {
    case TopicEvent(value) if value.event.isTopicCreated =>
      topic = Topic(value.name)
    case TopicEvent(value) if value.event.isTopicUpdated =>
      val topicUpdated = value.event.topicUpdated.get
      partitions =
        topicUpdated.getTopicDescription.topicPartitions.map { tp =>
          TopicPartition(
            value.name,
            tp.partition,
            tp.replicas.map { rep =>
              TopicPartitionReplica(
                rep.id,
                tp.leader match {
                  case None => false
                  case Some(node) => node.equals(rep)
                })
            })
        }.toList
      //TODO maintain topic metadata
    case TopicEvent(value) if value.event.isTopicDeleted =>
      status = EntityStatus.Removed
  }
}
