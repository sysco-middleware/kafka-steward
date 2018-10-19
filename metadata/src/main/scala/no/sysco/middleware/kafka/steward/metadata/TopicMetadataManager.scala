package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef}
import no.sysco.middleware.kafka.steward.proto.collector.TopicEvent

class TopicMetadataManager extends Actor with ActorLogging {

  var topics: Map[String, ActorRef] = Map()

  override def receive: Receive = {
    case topicEvent: TopicEvent => handleTopicEvent(topicEvent)
  }

  private def handleTopicEvent(topicEvent: TopicEvent): Unit = {
    topics.get(topicEvent.name) match {
      case Some(topicMetadata) => topicMetadata ! topicEvent
      case None =>
        val actorRef = context.actorOf(TopicMetadata.props())
        actorRef ! topicEvent
        topics = topics + (topicEvent.name -> actorRef)
    }
  }
}
