package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent

object GraphManager {
  def props(): Props = Props(new GraphManager())
}

class GraphManager() extends Actor with ActorLogging {

  val topicsManager: ActorRef = context.actorOf(TopicsManager.props())
  val clusterManager: ActorRef = context.actorOf(ClusterManager.props())

  override def receive: Receive = {
    case collectorEvent: CollectorEvent => handleCollectorEvent(collectorEvent)
  }

  private def handleCollectorEvent(collectorEvent: CollectorEvent): Unit = {
    collectorEvent.value match {
      case value: CollectorEvent.Value if value.isTopicEvent => topicsManager ! value.topicEvent.get
      case value: CollectorEvent.Value if value.isClusterEvent => clusterManager ! value.clusterEvent.get
      case value: CollectorEvent.Value if value.isBrokerEvent => clusterManager ! value.brokerEvent.get
    }
  }
}
