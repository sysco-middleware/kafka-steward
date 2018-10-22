package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.proto.collector.TopicEvent

object TopicsManager {
  def props(): Props = Props(new TopicsManager())
}

class TopicsManager extends Actor with ActorLogging {

  var topics: Map[String, ActorRef] = Map()

  override def receive: Receive = {
    case topicEvent: TopicEvent => handleTopicEvent(topicEvent)
  }

  private def handleTopicEvent(topicEvent: TopicEvent): Unit = {
    topics.get(topicEvent.name) match {
      case Some(topicRef) => topicRef ! topicEvent
      case None =>
        val actorRef = context.actorOf(TopicMetaManager.props())
        actorRef ! topicEvent
        topics = topics + (topicEvent.name -> actorRef)
    }
  }
}
