package no.sysco.middleware.kafka.event.collector

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager
import no.sysco.middleware.kafka.event.collector.internal.{ EventConsumer, EventProducer, EventRepository }
import no.sysco.middleware.kafka.event.collector.topic.TopicManager
import no.sysco.middleware.kafka.event.proto.collector.{ CollectorEvent, ClusterEvent, NodeEvent, TopicEvent }

import scala.concurrent.ExecutionContext

object Collector extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("collector")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  def props(): Props = Props(new Collector())

  actorSystem.actorOf(props())
}

/**
 * Main application actor. Manage entity managers to collect and publish events from a Kafka Cluster.
 *
 */
class Collector(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) extends Actor {
  val config: CollectorConfig = new CollectorConfig(ConfigFactory.load())

  val eventProducer: ActorRef = context.actorOf(EventProducer.props(config.Kafka.bootstrapServers, config.Collector.eventTopic))
  val eventRepository: ActorRef = context.actorOf(EventRepository.props(config.Kafka.bootstrapServers))

  val clusterManager: ActorRef = context.actorOf(ClusterManager.props(config.Collector.clusterPollInterval, eventRepository, eventProducer))
  val topicManager: ActorRef = context.actorOf(TopicManager.props(config.Collector.topicPollInterval, eventRepository, eventProducer))

  val eventConsumer: ActorRef = context.actorOf(EventConsumer.props(self, config.Kafka.bootstrapServers, config.Collector.eventTopic))

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
      case Some(clusterEventValue) => clusterManager ! clusterEventValue
      case None                    =>
    }
  }

  private def handleNodeEvent(nodeEvent: Option[NodeEvent]): Unit = {
    nodeEvent match {
      case Some(nodeEventValue) => clusterManager ! nodeEventValue
      case None                 =>
    }
  }

  private def handleTopicEvent(topicEvent: Option[TopicEvent]): Unit = {
    topicEvent match {
      case Some(topicEventValue) => topicManager ! topicEventValue
      case None                  =>
    }
  }

}
