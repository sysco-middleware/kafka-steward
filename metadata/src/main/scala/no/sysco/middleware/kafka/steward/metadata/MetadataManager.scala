package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import com.typesafe.config.ConfigFactory
import no.sysco.middleware.kafka.steward.metadata.internal.{ CollectorManager, EntityEventProducer }

object MetadataManager {
  def props(): Props = Props(new MetadataManager())
}

class MetadataManager() extends Actor with ActorLogging {
  val config: MetadataConfig = new MetadataConfig(ConfigFactory.load())

  val entityEventProducer: ActorRef = context.actorOf(EntityEventProducer.props(config.Kafka.bootstrapServers, config.Metadata.eventTopic), "entity-event-producer")
  val entityManager: ActorRef = context.actorOf(EntityManager.props(entityEventProducer), "entity-manager")
  val collectorManager: ActorRef = context.actorOf(CollectorManager.props(entityEventProducer), "collector-manager")

  override def receive: Receive = Actor.emptyBehavior
}
