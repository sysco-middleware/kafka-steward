package no.sysco.middleware.kafka.event.collector

import akka.actor.{ Actor, ActorRef, ActorSystem }
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager
import no.sysco.middleware.kafka.event.collector.internal.{ EventConsumer, EventProducer, EventRepository }
import no.sysco.middleware.kafka.event.collector.topic.TopicManager
import no.sysco.middleware.kafka.event.proto.collector.CollectorEvent

import scala.concurrent.ExecutionContext

object Collector {
  implicit val actorSystem: ActorSystem = ActorSystem("collector")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  new Collector()
}

/**
  *
  * @param actorSystem
  * @param actorMaterializer
  * @param executionContext
  */
class Collector(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) extends Actor {
  val config: CollectorConfig = new CollectorConfig(ConfigFactory.load())

  val eventProducer: ActorRef = context.actorOf(EventProducer.props(config.Kafka.bootstrapServers, config.Collector.eventTopic))
  val eventRepository: ActorRef = context.actorOf(EventRepository.props(config.Kafka.bootstrapServers))

  val clusterEventCollector: ActorRef = context.actorOf(ClusterManager.props(config.Collector.clusterPollInterval, eventRepository, eventProducer))
  val topicEventCollector: ActorRef = context.actorOf(TopicManager.props(config.Collector.topicPollInterval, eventRepository, eventProducer))

  val eventConsumer: ActorRef = context.actorOf(EventConsumer.props(self, config.Kafka.bootstrapServers, config.Collector.eventTopic))

  override def receive: Receive = {
    case collectorEvent: CollectorEvent =>
      collectorEvent.value match {
        case value: CollectorEvent.Value if value.isClusterEvent =>
          value.clusterEvent match {
            case Some(clusterEvent) =>
              clusterEventCollector ! clusterEvent
            case None =>
          }
        case value: CollectorEvent.Value if value.isNodeEvent =>
          value.nodeEvent match {
            case Some(nodeEvent) =>
              clusterEventCollector ! nodeEvent
            case None =>
          }
        case value: CollectorEvent.Value if value.isTopicEvent =>
          value.topicEvent match {
            case Some(topicEvent) =>
              topicEventCollector ! topicEvent
            case None =>
          }
      }
  }
}
