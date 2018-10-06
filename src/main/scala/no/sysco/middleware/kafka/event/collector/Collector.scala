package no.sysco.middleware.kafka.event.collector

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager.GetCluster
import no.sysco.middleware.kafka.event.collector.cluster.NodeManager.ListNodes
import no.sysco.middleware.kafka.event.collector.http.HttpCollectorQueryService
import no.sysco.middleware.kafka.event.collector.internal.{ EventConsumer, EventProducer, EventRepository }
import no.sysco.middleware.kafka.event.collector.topic.TopicManager
import no.sysco.middleware.kafka.event.collector.topic.TopicManager.ListTopics
import no.sysco.middleware.kafka.event.proto.collector.{ CollectorEvent, ClusterEvent, NodeEvent, TopicEvent }

import scala.concurrent.ExecutionContext

/**
 * Application entry point.
 */
object Collector extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("collector-system")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  def props(): Props = Props(new Collector())

  val collector = actorSystem.actorOf(props(), "collector")

  val httpCollectorQueryService = new HttpCollectorQueryService(collector)

  val bindingFuture = Http().bindAndHandle(httpCollectorQueryService.route, "0.0.0.0", 8080)

  sys.addShutdownHook(
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => actorSystem.terminate()))
}

/**
 * Main application actor. Manage entity managers to collect and publish events from a Kafka Cluster.
 */
class Collector(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext)
  extends Actor with ActorLogging {
  val config: CollectorConfig = new CollectorConfig(ConfigFactory.load())

  val eventProducer: ActorRef = context.actorOf(EventProducer.props(config.Kafka.bootstrapServers, config.Collector.eventTopic), "event-producer")
  val eventRepository: ActorRef = context.actorOf(EventRepository.props(config.Kafka.bootstrapServers), "event-repository")

  val clusterEventCollector: ActorRef = context.actorOf(ClusterManager.props(config.Collector.clusterPollInterval, eventRepository, eventProducer), "cluster-manager")
  val topicEventCollector: ActorRef = context.actorOf(TopicManager.props(config.Collector.topicPollInterval, eventRepository, eventProducer), "topic-manager")

  val eventConsumer: ActorRef = context.actorOf(EventConsumer.props(self, config.Kafka.bootstrapServers, config.Collector.eventTopic), "event-consumer")

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
      case Some(clusterEventValue) => clusterEventCollector ! clusterEventValue
      case None                    =>
    }
  }

  private def handleNodeEvent(nodeEvent: Option[NodeEvent]): Unit = {
    nodeEvent match {
      case Some(nodeEventValue) => clusterEventCollector ! nodeEventValue
      case None                 =>
    }
  }

  private def handleTopicEvent(topicEvent: Option[TopicEvent]): Unit = {
    topicEvent match {
      case Some(topicEventValue) => topicEventCollector ! topicEventValue
      case None                  =>
    }
  }

}
