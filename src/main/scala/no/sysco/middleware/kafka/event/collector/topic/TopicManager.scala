package no.sysco.middleware.kafka.event.collector.topic

import java.time.Duration

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.ActorMaterializer
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.event.collector.internal.Parser
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto.collector.{TopicCreated, TopicDeleted, TopicEvent}

import scala.concurrent.ExecutionContext

object TopicManager {
  def props(
             pollInterval: Duration,
             includeInternalTopics: Boolean = true,
             whitelistTopics: List[String] = List(),
             blacklistTopics: List[String] = List(),
             eventRepository: ActorRef,
             eventProducer: ActorRef)(implicit actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) =
    Props(new TopicManager(pollInterval, includeInternalTopics, whitelistTopics, blacklistTopics, eventRepository, eventProducer))

  case class ListTopics()

}

/**
  * Observe and publish Topic events.
  *
  * @param pollInterval          Frequency to poll topics from a Kafka Cluster.
  * @param includeInternalTopics If internal topics should be included or not.
  * @param whitelistTopics       list of topic names to include
  * @param blacklistTopics       list of topic names to do not include.
  * @param eventRepository       Reference to Repository where events are stored.
  * @param eventProducer         Reference to Events producer, to publish events.
  */
class TopicManager(
                    pollInterval: Duration,
                    includeInternalTopics: Boolean,
                    whitelistTopics: List[String],
                    blacklistTopics: List[String],
                    eventRepository: ActorRef,
                    eventProducer: ActorRef)
                  (implicit
                   actorMaterializer: ActorMaterializer,
                   val executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import TopicManager._
  import no.sysco.middleware.kafka.event.collector.internal.EventRepository._
  import no.sysco.middleware.kafka.event.collector.metrics.Metrics._

  var topicsAndDescription: Map[String, Option[TopicDescription]] = Map()

  override def preStart(): Unit = scheduleCollectTopics

  override def receive(): Receive = {
    case CollectTopics() => handleCollectTopics()
    case topicsCollected: TopicsCollected => handleTopicsCollected(topicsCollected)
    case topicDescribed: TopicDescribed => handleTopicDescribed(topicDescribed)
    case topicEvent: TopicEvent => handleTopicEvent(topicEvent)
    case ListTopics() => handleListTopics()
  }

  def handleCollectTopics(): Unit = {
    log.info("Handling collect topics command.")
    eventRepository ! CollectTopics()

    scheduleCollectTopics
  }

  private def scheduleCollectTopics = {
    context.system.scheduler.scheduleOnce(pollInterval, () => self ! CollectTopics())
  }

  /**
    * Handle current list of topics collected by first checking if any topic has been deleted
    * and then if any has been updated.
    */
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
        }
        evaluateCurrentTopics(ns, names)
    }
  }

  private def evaluateTopicsCollected(topicNames: List[String]): Unit = topicNames match {
    case Nil =>
    case name :: names =>
      // first, if topic is included on blacklist, then should be filtered
      if (blacklistTopics.nonEmpty) {
        if (blacklistTopics.contains(name)) {
          evaluateTopicsCollected(names)
          return
        }
      }
      // then if there are whitelist, then only if it is included we continue
      if (whitelistTopics.nonEmpty) {
        if (!whitelistTopics.contains(name)) {
          evaluateTopicsCollected(names)
          return
        }
      }
      // finally, topic is evaluated
      name match {
        case n if !topicsAndDescription.keys.exists(_.equals(n)) =>
          Stats.record(List(topicTypeTag, createdOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
          eventProducer ! TopicEvent(name, TopicEvent.Event.TopicCreated(TopicCreated()))
        case n if topicsAndDescription.keys.exists(_.equals(n)) =>
          eventRepository ! DescribeTopic(name)
      }
      evaluateTopicsCollected(names)
  }

  /**
    * Try to describe topic.
    * If internal topics are excluded and described topic is internal, method will exit.
    *
    * @param topicDescribed TopicDescribed information
    */
  def handleTopicDescribed(topicDescribed: TopicDescribed): Unit = topicDescribed.topicAndDescription match {
    case (topicName: String, topicDescription: TopicDescription) =>
      log.info("Handling topic {} described.", topicName)

      if (!includeInternalTopics) {
        if (topicDescription.internal) {
          log.warning("Internal topic excluded: {}", topicName)
          return
        }
      }

      // assume that assume, that topic name already collected, no null pointers
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

  def handleListTopics(): Unit = sender() ! Topics(topicsAndDescription)
}
