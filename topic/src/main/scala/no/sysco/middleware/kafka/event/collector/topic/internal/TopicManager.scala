package no.sysco.middleware.kafka.event.collector.topic.internal

import java.time.Duration

import akka.actor.{ Actor, ActorRef, Props }
import akka.stream.ActorMaterializer
import no.sysco.middleware.kafka.metadata.collector.proto.topic.{ TopicCreatedPb, TopicDeletedPb, TopicEventPb }

import scala.concurrent.ExecutionContext

object TopicManager {
  def props(pollInterval: Duration, bootstrapServers: String, topicEventTopic: String)(implicit actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) =
    Props(new TopicManager(pollInterval, bootstrapServers, topicEventTopic))
}

/**
 * Observe and publish Topic events.
 *
 * @param pollInterval     Frequency to poll topics from a Kafka Cluster.
 * @param bootstrapServers Kafka Bootstrap Servers.
 * @param topicEventTopic  Topic where Events are stored.
 */
class TopicManager(pollInterval: Duration, bootstrapServers: String, topicEventTopic: String)(implicit actorMaterializer: ActorMaterializer, val executionContext: ExecutionContext)
  extends Actor {

  val topicEventProducer: ActorRef = context.actorOf(TopicEventProducer.props(bootstrapServers, topicEventTopic))
  val topicRepository: ActorRef = context.actorOf(TopicRepository.props(bootstrapServers))
  val topicEventConsumer: ActorRef = context.actorOf(TopicEventConsumer.props(self, bootstrapServers, topicEventTopic))
  var topicsAndDescription: Map[String, Option[Description]] = Map()

  def evaluateCurrentTopics(currentNames: List[String], names: List[String]): Unit = {
    currentNames match {
      case Nil =>
      case name :: ns =>
        if (!names.contains(name))
          topicEventProducer ! TopicEventPb(name, TopicEventPb.Event.TopicDeleted(TopicDeletedPb()))
        evaluateCurrentTopics(ns, names)
    }
  }

  def handleTopicsCollected(topicsCollected: TopicsCollected): Unit = {
    val topics = topicsCollected.names.filter(_.equals(topicEventTopic))
    evaluateCurrentTopics(topicsAndDescription.keys.toList, topics)
    evaluateTopicsCollected(topics)
  }

  def evaluateTopicsCollected(topicNames: List[String]): Unit = topicNames match {
    case Nil =>
    case name :: names =>
      name match {
        case n if !topicsAndDescription.keys.exists(_.equals(n)) =>
          topicEventProducer ! TopicEventPb(name, TopicEventPb.Event.TopicCreated(TopicCreatedPb()))
        case n if topicsAndDescription.keys.exists(_.equals(n)) =>
          topicRepository ! DescribeTopic(name)
      }
      evaluateTopicsCollected(names)
  }

  def handleTopicEvent(topicEvent: TopicEventPb): Unit =
    topicEvent.event match {
      case event if event.isTopicCreated =>
        event.topicCreated match {
          case Some(_) =>
            topicRepository ! DescribeTopic(topicEvent.name)
            topicsAndDescription = topicsAndDescription + (topicEvent.name -> None)
          case None =>
        }
      case event if event.isTopicUpdated =>
        event.topicUpdated match {
          case Some(topicUpdated) =>
            val topicDescription = Some(Parser.fromPb(topicEvent.name, topicUpdated.topicDescription.get))
            topicsAndDescription = topicsAndDescription + (topicEvent.name -> topicDescription)
          case None =>
        }

      case event if event.isTopicDeleted =>
        event.topicDeleted match {
          case Some(_) =>
            topicsAndDescription = topicsAndDescription - topicEvent.name
          case None =>
        }
    }

  def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = topicDescribed.topicAndDescription match {
    case (name: String, description: Description) =>
      topicsAndDescription(name) match {
        case None =>
          topicEventProducer ! TopicEventPb(name, TopicEventPb.Event.TopicUpdated(Parser.toPb(description)))
        case Some(current) =>
          if (!current.equals(description))
            topicEventProducer ! TopicEventPb(name, TopicEventPb.Event.TopicUpdated(Parser.toPb(description)))
      }
  }

  def handleCollectTopics(): Unit = {
    topicRepository ! CollectTopics()

    context.system.scheduler.scheduleOnce(pollInterval, () => self ! CollectTopics())
  }

  override def preStart(): Unit = self ! CollectTopics()

  def handleListTopics(): Unit =
    sender() ! Topics(topicsAndDescription)

  override def receive: Receive = {
    case topicsCollected: TopicsCollected => handleTopicsCollected(topicsCollected)
    case topicDescribed: TopicDescribed => handleTopicDescribed(topicDescribed)
    case topicEvent: TopicEventPb => handleTopicEvent(topicEvent)
    case CollectTopics() => handleCollectTopics()
    case ListTopics() => handleListTopics()
  }

}
