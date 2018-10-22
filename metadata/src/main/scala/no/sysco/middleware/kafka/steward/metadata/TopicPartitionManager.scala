package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, Props}
import no.sysco.middleware.kafka.steward.metadata.TopicPartitionManager.TopicPartitionUpdated
import no.sysco.middleware.kafka.steward.metadata.model.{TopicPartition, TopicPartitionReplica}
import no.sysco.middleware.kafka.steward.proto.collector.TopicDescription.TopicPartitionInfo

object TopicPartitionManager {

  def props(topicName: String, topicPartitionInfo: TopicPartitionInfo): Props =
    Props(new TopicPartitionManager(topicName, topicPartitionInfo))

  case class TopicPartitionCreated(topicName: String, topicPartitionInfo: TopicPartitionInfo)

  case class TopicPartitionUpdated(topicName: String, topicPartitionInfo: TopicPartitionInfo)
}

class TopicPartitionManager(topicName: String, topicPartitionInfo: TopicPartitionInfo) extends Actor with ActorLogging {

  var topicPartition: TopicPartition = TopicPartition(topicName, topicPartitionInfo.partition)
  var replicas: Seq[TopicPartitionReplica] =
    topicPartitionInfo.replicas.map { replica =>
      TopicPartitionReplica(
        replica.id,
        topicPartitionInfo.leader match {
          case None => false
          case Some(node) => node.equals(replica)
        })
    }

  override def receive: Receive = {
    case topicPartitionUpdated: TopicPartitionUpdated => handleTopicPartitionUpdated(topicPartitionUpdated)
  }

  private def handleTopicPartitionUpdated(topicPartitionUpdated: TopicPartitionUpdated): Unit = {
    topicPartition =
      TopicPartition(
        topicPartitionUpdated.topicName,
        topicPartitionUpdated.topicPartitionInfo.partition)
  }
}
