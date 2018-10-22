package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.metadata.model.EntityStatus.EntityStatus
import no.sysco.middleware.kafka.steward.metadata.model.{Cluster, EntityStatus, Metadata}
import no.sysco.middleware.kafka.steward.proto.collector.{BrokerEvent, ClusterEvent}

object ClusterManager {
  def props(): Props = Props(new ClusterManager)
}

class ClusterManager extends Actor with ActorLogging {

  var cluster: Cluster = _
  var brokers: Map[String, ActorRef] = Map()
  var status: EntityStatus = EntityStatus.Current

  override def receive: Receive = {
    case clusterEvent: ClusterEvent => handleClusterEvent(clusterEvent)
    case brokerEvent: BrokerEvent => handleBrokerEvent(brokerEvent)
  }

  private def handleClusterEvent(clusterEvent: ClusterEvent): Unit = clusterEvent.event match {
    case event: ClusterEvent.Event if event.isClusterCreated =>
      cluster = Cluster(clusterEvent.id)
    case event: ClusterEvent.Event if event.isClusterUpdated => //???
  }

  private def handleBrokerEvent(brokerEvent: BrokerEvent): Unit = brokerEvent.event match {
    case event: BrokerEvent.Event if event.isBrokerCreated =>
//      val brokerCreated = event.brokerCreated.get
      val brokerRef = context.actorOf(BrokerMetadata.props(brokerEvent.id))
      brokers = brokers + (brokerEvent.id -> brokerRef)
    case event: BrokerEvent.Event if event.isBrokerUpdated =>
//      val brokerUpdated = event.brokerUpdated.get
      brokers.get(brokerEvent.id) match {
        case None => //?
        case Some(brokerRef) => //?
      }
  }
}
