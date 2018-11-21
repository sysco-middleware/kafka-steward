package no.sysco.middleware.kafka.steward.collector.topic.core

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.topic.core.Topics.GetState
import no.sysco.middleware.kafka.steward.collector.topic.core.model._

import scala.annotation.tailrec

object Topics {

  def props(clusterId: ClusterId, origin: ActorRef): Props = Props(new Topics(clusterId, origin))

  case class GetState()
}

/**
 * Topics Actor, maintaining state of current topics in a cluster.
 * @param clusterId Cluster identifier.
 * @param origin Origin repository reference.
 */
class Topics(clusterId: ClusterId, origin: ActorRef) extends Actor with ActorLogging {

  // Topics state.
  private var state: Map[String, Topic] = Map()

  override def receive: Receive = {
    case topicsCollected: TopicsCollected => handleTopicsCollected(topicsCollected)
    case topicDescribed: TopicDescribed => handleTopicDescribed(topicDescribed)
    case topicUpdated: TopicUpdated => handleTopicUpdated(topicUpdated)
    case topicDeleted: TopicDeleted => handleTopicDeleted(topicDeleted)
    case GetState() => sender() ! state
  }

  /**
   * Validate new topics and topics removed.
   * @param topicsCollected List of current topics.
   */
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
          case None => origin ! TopicDeleted(clusterId, topic)
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
          case None => origin ! TopicCreated(clusterId, topic)
          case Some(_) => // do nothing
        }
        checkTopicsCreated(topics)
    }

  /**
   * Check if Topic configuration has changed, to propagate update.
   * @param topicDescribed Current topic configuration.
   */
  private def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = {
    state.get(topicDescribed.name) match {
      case Some(topic) =>
        if (topic.equals(topicDescribed.topic)) origin ! TopicUpdated(clusterId, topic)
      case None => origin ! TopicUpdated(clusterId, topicDescribed.topic)
    }
  }

  /**
   * Update Topic state.
   * @param topicUpdated Topic updated.
   */
  private def handleTopicUpdated(topicUpdated: TopicUpdated): Unit =
    state = state + (topicUpdated.topic.name -> topicUpdated.topic)

  /**
   * Remove Topic from state.
   * @param topicDeleted Topic deleted.
   */
  private def handleTopicDeleted(topicDeleted: TopicDeleted): Unit = state = state - topicDeleted.name

}
