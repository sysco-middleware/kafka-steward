package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.metadata.TopicMetaManager.TopicRemoved
import no.sysco.middleware.kafka.steward.metadata.model.EntityStatus.EntityStatus
import no.sysco.middleware.kafka.steward.metadata.model.{EntityStatus, Metadata, Topic}
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent.Value.TopicEvent
import no.sysco.middleware.kafka.steward.proto.collector.TopicDescription.TopicPartitionInfo

import scala.annotation.tailrec

object TopicMetaManager {
  def props(): Props = Props(new TopicMetaManager)

  case class TopicRemoved()
}

class TopicMetaManager extends Actor with ActorLogging {

  var topic: Topic = _
  var partitions: Map[Int, ActorRef] = Map()
  var metadata: Metadata = Metadata()
  var status: EntityStatus = EntityStatus.Current

  override def receive: Receive = {
    case topicEvent: TopicEvent => handleTopicEvent(topicEvent)
  }

  private def handleTopicEvent(topicEvent: TopicEvent): Unit = topicEvent match {
    case TopicEvent(value) if value.event.isTopicCreated => topic = Topic(value.name)
    case TopicEvent(value) if value.event.isTopicUpdated =>
      val topicUpdated = value.event.topicUpdated.get
      validateTopicPartitions(value.name, topicUpdated.getTopicDescription.topicPartitions.toList)
    case TopicEvent(value) if value.event.isTopicDeleted => status =
      EntityStatus.Removed
      partitions.values.foreach { partitionRef => partitionRef ! TopicRemoved() }
  }

  @tailrec
  private def validateTopicPartitions(topicName: String, topicPartitions: List[TopicPartitionInfo]): Unit = {
    topicPartitions match {
      case Nil =>
      case tp :: tps =>
        partitions.get(tp.partition) match {
          case None =>
            val partitionRef = context.actorOf(TopicPartitionManager.props(topicName, tp))
            partitions = partitions + (tp.partition -> partitionRef)
          case Some(_) => //Partition already mapped
        }
        validateTopicPartitions(topicName, tps)
    }
  }
}
