package no.sysco.middleware.kafka.steward.metadata.internal

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.metadata.EntityManager.OpMetadataUpdated
import no.sysco.middleware.kafka.steward.metadata.model.{Broker, OpMetadata, Topic}
import no.sysco.middleware.kafka.steward.proto.collector.{BrokerEvent, ClusterEvent, CollectorEvent, TopicEvent}

object CollectorManager {

  def props(entityManager: ActorRef): Props = Props(new CollectorManager(entityManager))
}

class CollectorManager(entityManager: ActorRef) extends Actor with ActorLogging {


  override def receive: Receive = {
    case collectorEvent: CollectorEvent => handleCollectorEvent(collectorEvent)
  }

  def handleCollectorEvent(collectorEvent: CollectorEvent): Unit = {
    collectorEvent.value match {
      case value if value.isBrokerEvent => handleBrokerEvent(value.brokerEvent.get)
      case value if value.isClusterEvent => handleClusterEvent(value.clusterEvent.get)
      case value if value.isTopicEvent => handleTopicEvent(value.topicEvent.get)
    }
  }

  def handleBrokerEvent(brokerEvent: BrokerEvent): Unit = {
    brokerEvent.event match {
      case event if event.isBrokerCreated =>
        val brokerCreated = event.brokerCreated.get
        val node = brokerCreated.node.get
        //TODO val config = brokerCreated.config.get
        entityManager !
          OpMetadataUpdated(
            new Broker(brokerEvent.id),
            OpMetadata(
              Map(
                "host" -> node.host,
                "port" -> node.port.toString)))
      case event if event.brokerUpdated =>
        val brokerUpdated = event.brokerUpdated.get
        val node = brokerUpdated.node.get
        entityManager !
          OpMetadataUpdated(
            new Broker(brokerEvent.id),
            OpMetadata(
              Map(
                "host" -> node.host,
                "port" -> node.port.toString)))
    }
  }

  def handleClusterEvent(clusterEvent: ClusterEvent): Unit = {
    clusterEvent.event match {
      case event if event.isClusterCreated =>
      //TODO how to link cluster with brokers?
      case event if event.isClusterUpdated =>
      //TODO how to link cluster with brokers?
    }
  }

  def handleTopicEvent(topicEvent: TopicEvent): Unit = {
    topicEvent.event match {
      case event if event.isTopicCreated =>
        entityManager !
          OpMetadataUpdated(
            new Topic(topicEvent.name),
            OpMetadata())
      case event if event.isTopicUpdated =>
        val topicUpdated = event.topicUpdated.get
        val partitions = topicUpdated.getTopicDescription.topicPartitions
        entityManager !
          OpMetadataUpdated(
            new Topic(topicEvent.name),
            OpMetadata(
              Map(
                "partitions" -> partitions.size.toString,
                "replication_factor" -> partitions.head.replicas.size.toString
              )))
      case event if event.isTopicDeleted =>

    }
  }
}
