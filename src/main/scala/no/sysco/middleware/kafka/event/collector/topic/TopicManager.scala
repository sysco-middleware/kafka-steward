package no.sysco.middleware.kafka.event.collector.topic

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.stream.ActorMaterializer
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto.collector.{ TopicCreated, TopicDeleted, TopicEvent }

import scala.concurrent.ExecutionContext

object TopicManager {
  def props(pollInterval: Duration, eventRepository: ActorRef, eventProducer: ActorRef)(implicit actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) =
    Props(new TopicManager(pollInterval, eventRepository, eventProducer))

  case class ListTopics()
}

/**
 * Observe and publish Topic events.
 *
 * @param pollInterval     Frequency to poll topics from a Kafka Cluster.
 * @param eventRepository Reference to Repository where events are stored.
 * @param eventProducer   Reference to Events producer, to publish events.
 */
class TopicManager(pollInterval: Duration, eventRepository: ActorRef, eventProducer: ActorRef)(implicit actorMaterializer: ActorMaterializer, val executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import TopicManager._
  import no.sysco.middleware.kafka.event.collector.internal.EventRepository._

  var topicsAndDescription: Map[String, Option[TopicDescription]] = Map()

  def handleTopicsCollected(topicsCollected: TopicsCollected): Unit = {
    log.info(s"Handling ${topicsCollected.names.size} topics collected.")
    evaluateCurrentTopics(topicsAndDescription.keys.toList, topicsCollected.names)
    evaluateTopicsCollected(topicsCollected.names)
  }

  private def evaluateCurrentTopics(currentNames: List[String], names: List[String]): Unit = {
    currentNames match {
      case Nil =>
      case name :: ns =>
        if (!names.contains(name))
          eventProducer ! TopicEvent(name, TopicEvent.Event.TopicDeleted(TopicDeleted()))
        evaluateCurrentTopics(ns, names)
    }
  }

  private def evaluateTopicsCollected(topicNames: List[String]): Unit = topicNames match {
    case Nil =>
    case name :: names =>
      name match {
        case n if !topicsAndDescription.keys.exists(_.equals(n)) =>
          eventProducer ! TopicEvent(name, TopicEvent.Event.TopicCreated(TopicCreated()))
        case n if topicsAndDescription.keys.exists(_.equals(n)) =>
          eventRepository ! DescribeTopic(name)
      }
      evaluateTopicsCollected(names)
  }

  def handleTopicEvent(topicEvent: TopicEvent): Unit = {
    log.info("Handling topic {} event.", topicEvent.name)
    topicEvent.event match {
      case event if event.isTopicCreated =>
        event.topicCreated match {
          case Some(_) =>
            eventRepository ! DescribeTopic(topicEvent.name)
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
  }

  def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = topicDescribed.topicAndDescription match {
    case (name: String, description: TopicDescription) =>
      log.info("Handling topic {} described.", name)
      topicsAndDescription(name) match {
        case None =>
          eventProducer ! TopicEvent(name, TopicEvent.Event.TopicUpdated(Parser.toPb(description)))
        case Some(current) =>
          if (!current.equals(description))
            eventProducer ! TopicEvent(name, TopicEvent.Event.TopicUpdated(Parser.toPb(description)))
      }
  }

  def handleCollectTopics(): Unit = {
    log.info("Handling collect topics command.")
    eventRepository ! CollectTopics()

    scheduleCollectTopics
  }

  private def scheduleCollectTopics = {
    context.system.scheduler.scheduleOnce(pollInterval, () => self ! CollectTopics())
  }

  def handleListTopics(): Unit = sender() ! Topics(topicsAndDescription)

  override def preStart(): Unit = scheduleCollectTopics

  override def receive(): Receive = {
    case topicsCollected: TopicsCollected => handleTopicsCollected(topicsCollected)
    case topicDescribed: TopicDescribed   => handleTopicDescribed(topicDescribed)
    case topicEvent: TopicEvent           => handleTopicEvent(topicEvent)
    case CollectTopics()                  => handleCollectTopics()
    case ListTopics()                     => handleListTopics()
  }
}
