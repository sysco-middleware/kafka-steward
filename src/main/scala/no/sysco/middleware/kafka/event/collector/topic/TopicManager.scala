package no.sysco.middleware.kafka.event.collector.topic

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.stream.ActorMaterializer
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.event.collector.internal.Parser
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto.collector.{ TopicCreated, TopicDeleted, TopicEvent }

import scala.concurrent.ExecutionContext

object TopicManager {
  def props(
    pollInterval: Duration,
    includeInternalTopics: Boolean,
    eventRepository: ActorRef,
    eventProducer: ActorRef)(implicit actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) =
    Props(new TopicManager(pollInterval, includeInternalTopics, eventRepository, eventProducer))

  case class ListTopics()
}

/**
 * Observe and publish Topic events.
 *
 * @param pollInterval     Frequency to poll topics from a Kafka Cluster.
 * @param eventRepository Reference to Repository where events are stored.
 * @param eventProducer   Reference to Events producer, to publish events.
 */
class TopicManager(pollInterval: Duration, includeInternalTopics: Boolean, eventRepository: ActorRef, eventProducer: ActorRef)(implicit actorMaterializer: ActorMaterializer, val executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import TopicManager._
  import no.sysco.middleware.kafka.event.collector.internal.EventRepository._
  import no.sysco.middleware.kafka.event.collector.metrics.Metrics._

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
        if (!names.contains(name)) {
          Stats.record(List(topicTypeTag, deletedOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
          eventProducer ! TopicEvent(name, TopicEvent.Event.TopicDeleted(TopicDeleted()))
          evaluateCurrentTopics(ns, names)
        }
    }
  }

  private def evaluateTopicsCollected(topicNames: List[String]): Unit = topicNames match {
    case Nil =>
    case name :: names =>
      name match {
        case n if !topicsAndDescription.keys.exists(_.equals(n)) =>
          Stats.record(List(topicTypeTag, createdOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
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
        Stats.record(List(topicTypeTag, createdOperationTypeTag), Measurement.double(totalMessageConsumedMeasure, 1))
        event.topicCreated match {
          case Some(_) =>
            topicsAndDescription = topicsAndDescription + (topicEvent.name -> None)
            eventRepository ! DescribeTopic(topicEvent.name)
          case None =>
        }
      case event if event.isTopicUpdated =>
        Stats.record(List(topicTypeTag, updatedOperationTypeTag), Measurement.double(totalMessageConsumedMeasure, 1))
        event.topicUpdated match {
          case Some(topicUpdated) =>
            val topicDescription = Some(Parser.fromPb(topicEvent.name, topicUpdated.topicDescription.get))
            topicsAndDescription = topicsAndDescription + (topicEvent.name -> topicDescription)
          case None =>
        }
      case event if event.isTopicDeleted =>
        Stats.record(List(topicTypeTag, deletedOperationTypeTag), Measurement.double(totalMessageConsumedMeasure, 1))
        event.topicDeleted match {
          case Some(_) =>
            topicsAndDescription = topicsAndDescription - topicEvent.name
          case None =>
        }
    }
  }

  //  def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = {
  //    log.info("Handling topic: {} , internal: {}", topicDescribed.topicAndDescription._1, topicDescribed.topicAndDescription._2.internal)
  //    val filteredTopicDescribed = filterInternalTopic(topicDescribed).map(desc => desc.topicAndDescription).getOrElse(None)
  //
  //    filteredTopicDescribed match {
  //      case (topicName: String, topicDescription: TopicDescription) =>
  //        topicsAndDescription(topicName) match {
  //          case None =>
  //            Stats.record(List(topicTypeTag, createdOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
  //            eventProducer ! TopicEvent(topicName, TopicEvent.Event.TopicUpdated(Parser.toPb(topicDescription)))
  //          case Some(current) =>
  //            if (!current.equals(topicDescription)) {
  //              Stats.record(List(topicTypeTag, updatedOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
  //              eventProducer ! TopicEvent(topicName, TopicEvent.Event.TopicUpdated(Parser.toPb(topicDescription)))
  //            }
  //        }
  //      // remove from list
  //      case None => topicsAndDescription = topicsAndDescription - topicDescribed.topicAndDescription._1
  //    }
  //  }

  def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = topicDescribed.topicAndDescription match {
    case (topicName: String, topicDescription: TopicDescription) =>
      log.info("Handling topic {} described.", topicName)

      if (!includeInternalTopics) {
        if (topicDescription.internal) {
          log.warning("Internal topic excluded: {}", topicName)
          return
        }
      }

      topicsAndDescription(topicName) match {
        case None =>
          Stats.record(List(topicTypeTag, createdOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
          eventProducer ! TopicEvent(topicName, TopicEvent.Event.TopicUpdated(Parser.toPb(topicDescription)))
        case Some(current) =>
          if (!current.equals(topicDescription)) {
            Stats.record(List(topicTypeTag, updatedOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
            eventProducer ! TopicEvent(topicName, TopicEvent.Event.TopicUpdated(Parser.toPb(topicDescription)))
          }
      }
  }

  //  private def filterInternalTopic(topicDescribed: TopicDescribed): Option[TopicDescribed] = {
  //    if (topicDescribed.topicAndDescription._2.internal == includeInternalTopics) Option(topicDescribed) else Option.empty
  //  }

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
