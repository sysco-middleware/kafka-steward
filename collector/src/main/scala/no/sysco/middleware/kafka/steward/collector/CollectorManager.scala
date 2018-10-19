package no.sysco.middleware.kafka.steward.collector

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import no.sysco.middleware.kafka.steward.collector.cluster.BrokerManager.ListBrokers
import no.sysco.middleware.kafka.steward.collector.cluster.ClusterManager
import no.sysco.middleware.kafka.steward.collector.cluster.ClusterManager.GetCluster
import no.sysco.middleware.kafka.steward.collector.internal.{EventConsumer, EventProducer, EventRepository}
import no.sysco.middleware.kafka.steward.collector.topic.TopicManager
import no.sysco.middleware.kafka.steward.collector.topic.TopicManager.ListTopics
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent

import scala.concurrent.ExecutionContext

object CollectorManager {

  def props()(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext): Props = Props(new CollectorManager())
}

/**
  * Main application actor. Manage entity managers to collect and publish events from a Kafka Cluster.
  */
class CollectorManager(implicit
                       actorSystem: ActorSystem,
                       actorMaterializer: ActorMaterializer,
                       executionContext: ExecutionContext)
  extends Actor with ActorLogging {
  val config: CollectorConfig = new CollectorConfig(ConfigFactory.load())

  val eventProducer: ActorRef =
    context.actorOf(
      EventProducer.props(
        config.Kafka.bootstrapServers,
        config.Collector.eventTopic),
      "event-producer")
  val eventRepository: ActorRef =
    context.actorOf(EventRepository.props(config.Kafka.bootstrapServers), "event-repository")

  val clusterEventCollector: ActorRef =
    context.actorOf(
      ClusterManager.props(
        config.Collector.Cluster.pollInterval,
        eventRepository,
        eventProducer),
      "cluster-manager")
  val topicEventCollector: ActorRef =
    context.actorOf(
      TopicManager.props(
        config.Collector.Topic.pollInterval,
        config.Collector.Topic.includeInternalTopics,
        config.Collector.Topic.whitelist,
        config.Collector.Topic.blacklist,
        eventRepository,
        eventProducer),
      "topic-manager")

  val eventConsumer: ActorRef =
    context.actorOf(
      EventConsumer.props(
        self,
        config.Kafka.bootstrapServers,
        config.Collector.eventTopic),
      "event-consumer")

  override def receive: Receive = {
    case collectorEvent: CollectorEvent => handleEvent(collectorEvent)
    case getCluster: GetCluster => clusterEventCollector forward getCluster
    case listNodes: ListBrokers => clusterEventCollector forward listNodes
    case listTopics: ListTopics => topicEventCollector forward listTopics
  }

  private def handleEvent(event: CollectorEvent): Unit = {
    event.value match {
      case value: CollectorEvent.Value if value.isClusterEvent =>
        clusterEventCollector ! value.clusterEvent.get
      case value: CollectorEvent.Value if value.isBrokerEvent =>
        clusterEventCollector ! value.brokerEvent.get
      case value: CollectorEvent.Value if value.isTopicEvent =>
        topicEventCollector ! value.topicEvent.get
    }
  }
}
