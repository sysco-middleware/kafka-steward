package no.sysco.middleware.kafka.steward.collector.topic.core

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.topic.core.model._

import scala.annotation.tailrec

object Topics {

  def props(clusterId: ClusterId, remote: ActorRef): Props = Props(new Topics(clusterId, remote))

}

class Topics(clusterId: ClusterId, remote: ActorRef) extends Actor with ActorLogging {

  private var state: Map[String, Topic] = Map()

  override def receive: Receive = {
    case topicsCollected: TopicsCollected => handleTopicsCollected(topicsCollected)
    case topicDescribed: TopicDescribed => handleTopicDescribed(topicDescribed)
    case topicUpdated: TopicUpdated => handleTopicUpdated(topicUpdated)
    case topicDeleted: TopicDeleted => handleTopicDeleted(topicDeleted)
  }

  private def handleTopicsCollected(topicsCollected: TopicsCollected): Unit = {
    checkTopicsRemoved(state.keys.toList, topicsCollected.topics)
    checkTopicsCreated(topicsCollected.topics)
  }

  @tailrec
  private final def checkTopicsRemoved(currentTopics: List[String], topicsCollected: List[String]): Unit =
    currentTopics match {
      case Nil => // do nothing
      case topic :: topics =>
        topicsCollected.find(_.equals(topic)) match {
          case None => remote ! TopicDeleted(clusterId, topic)
          case Some(_) => // do nothing
        }
        checkTopicsRemoved(topics, topicsCollected)
    }

  @tailrec
  private final def checkTopicsCreated(topicsCollected: List[String]): Unit =
    topicsCollected match {
      case Nil => // do nothing
      case topic :: topics =>
        state.keys.find(_.equals(topic)) match {
          case None => remote ! TopicCreated(clusterId, topic)
          case Some(_) => // do nothing
        }
        checkTopicsCreated(topics)
    }

  private def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = {
    state.get(topicDescribed.name) match {
      case Some(topic) =>
        if (topic.equals(topicDescribed.topic)) remote ! TopicUpdated(clusterId, topic)
      case None => remote ! TopicUpdated(clusterId, topicDescribed.topic)
    }
  }

  private def handleTopicUpdated(topicUpdated: TopicUpdated): Unit =
    state = state + (topicUpdated.topic.name -> topicUpdated.topic)

  private def handleTopicDeleted(topicDeleted: TopicDeleted): Unit = state = state - topicDeleted.name

}
