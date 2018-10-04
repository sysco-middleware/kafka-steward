package no.sysco.middleware.kafka.event.collector.topic

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.ActorMaterializer
import com.typesafe.config.{ Config, ConfigFactory }
import no.sysco.middleware.kafka.event.collector.topic.internal.TopicManager

import scala.concurrent.ExecutionContext

object TopicEventCollector extends App {
  implicit val system: ActorSystem = ActorSystem("kafka-metadata-collector-topic")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  new TopicEventCollector()
}

/**
 * Collect Topic events by observing changes a Kafka Cluster.
 */
class TopicEventCollector(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer) {

  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val config: Config = ConfigFactory.load()
  val appConfig: TopicEventCollectorConfig = new TopicEventCollectorConfig(config)

  val topicManager: ActorRef =
    actorSystem.actorOf(
      TopicManager.props(
        appConfig.Collector.pollInterval,
        appConfig.Kafka.bootstrapServers,
        appConfig.Collector.topicEventTopic))

}
