package no.sysco.middleware.kafka.event.collector

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.opencensus.scala.Stats
import io.opencensus.scala.stats._
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager
import no.sysco.middleware.kafka.event.collector.internal.{EventConsumer, EventProducer, EventRepository}
import no.sysco.middleware.kafka.event.collector.topic.TopicManager
import no.sysco.middleware.kafka.event.proto.collector.CollectorEvent.EntityType
import no.sysco.middleware.kafka.event.proto.collector.{ClusterEvent, CollectorEvent, NodeEvent, TopicEvent}

import scala.concurrent.ExecutionContext
import scala.util.Try

object CollectorManager {

  def props()(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext): Props = Props(new CollectorManager())
}

/**
 * Main application actor. Manage entity managers to collect and publish events from a Kafka Cluster.
 */
class CollectorManager(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext)
  extends Actor with ActorLogging {
  val config: CollectorConfig = new CollectorConfig(ConfigFactory.load())

  val eventProducer: ActorRef = context.actorOf(EventProducer.props(config.Kafka.bootstrapServers, config.Collector.eventTopic), "event-producer")
  val eventRepository: ActorRef = context.actorOf(EventRepository.props(config.Kafka.bootstrapServers), "event-repository")

  val clusterEventCollector: ActorRef = context.actorOf(ClusterManager.props(config.Collector.clusterPollInterval, eventRepository, eventProducer), "cluster-manager")
  val topicEventCollector: ActorRef = context.actorOf(TopicManager.props(config.Collector.topicPollInterval, eventRepository, eventProducer), "topic-manager")

  val eventConsumer: ActorRef = context.actorOf(EventConsumer.props(self, config.Kafka.bootstrapServers, config.Collector.eventTopic), "event-consumer")

  val entityTypeTagName = "entity_type"
  val tryTotalMessageReceivedMeasure: Try[MeasureDouble] = for {
    // Create a new measure
    measure <- Measure.double("my_measure", "what a cool measure", "by")
    // Create & register a view which aggregates this measure
    view <- View("my_view",
      "view description",
      measure,
      List(entityTypeTagName),
      Sum)
    _ <- Stats.registerView(view)
  } yield measure
  val totalMessageReceivedMeasure: MeasureDouble = tryTotalMessageReceivedMeasure.get
  val clusterTypeTag: Tag = Tag(entityTypeTagName, EntityType.CLUSTER.name).get
  val nodeTypeTag: Tag = Tag(entityTypeTagName, EntityType.NODE.name).get
  val topicTypeTag: Tag = Tag(entityTypeTagName, EntityType.TOPIC.name).get

  override def receive: Receive = {
    case collectorEvent: CollectorEvent => handleEvent(collectorEvent)
    case _                              => // log unexpected message
  }

  private def handleEvent(event: CollectorEvent): Unit = {
    event.value match {
      case value: CollectorEvent.Value if value.isClusterEvent => handleClusterEvent(value.clusterEvent)
      case value: CollectorEvent.Value if value.isNodeEvent => handleNodeEvent(value.nodeEvent)
      case value: CollectorEvent.Value if value.isTopicEvent => handleTopicEvent(value.topicEvent)
      case _ => None // log unexpected event
    }
  }

  private def handleClusterEvent(clusterEvent: Option[ClusterEvent]): Unit = {
    clusterEvent match {
      case Some(clusterEventValue) =>
        Stats.record(List(clusterTypeTag), Measurement.double(totalMessageReceivedMeasure, 1))
        clusterEventCollector ! clusterEventValue
      case None                    =>
    }
  }

  private def handleNodeEvent(nodeEvent: Option[NodeEvent]): Unit = {
    nodeEvent match {
      case Some(nodeEventValue) =>
        Stats.record(List(nodeTypeTag), Measurement.double(totalMessageReceivedMeasure, 1))
        clusterEventCollector ! nodeEventValue
      case None                 =>
    }
  }

  private def handleTopicEvent(topicEvent: Option[TopicEvent]): Unit = {
    topicEvent match {
      case Some(topicEventValue) =>
        Stats.record(List(topicTypeTag), Measurement.double(totalMessageReceivedMeasure, 1))
        topicEventCollector ! topicEventValue
      case None                  =>
    }
  }

}
