package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef}
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent

object MetadataManager {

}

class MetadataManager(topicMetadataManager: ActorRef,
                      clusterMetadataManager: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = {
    case collectorEvent: CollectorEvent => handleCollectorEvent(collectorEvent)
  }

  private def handleCollectorEvent(collectorEvent: CollectorEvent): Unit = {
    collectorEvent.value match {
      case value: CollectorEvent.Value if value.isClusterEvent =>
        clusterMetadataManager ! value.clusterEvent.get
      case value: CollectorEvent.Value if value.isBrokerEvent =>
        clusterMetadataManager ! value.brokerEvent.get
      case value: CollectorEvent.Value if value.isTopicEvent =>
        topicMetadataManager ! value.topicEvent.get
    }
  }
}
