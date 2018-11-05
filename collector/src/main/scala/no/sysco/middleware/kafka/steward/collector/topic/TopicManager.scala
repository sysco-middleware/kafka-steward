package no.sysco.middleware.kafka.steward.collector.topic

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.steward.collector.internal.OriginRepository
import no.sysco.middleware.kafka.steward.collector.internal.Parser._
import no.sysco.middleware.kafka.steward.collector.model._
import no.sysco.middleware.kafka.steward.proto.collector.{ TopicCreated, TopicDeleted, TopicEvent }

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{ Failure, Success }

object TopicManager {
  def props(
    pollInterval: Duration,
    includeInternalTopics: Boolean = true,
    whitelistTopics: List[String] = List(),
    blacklistTopics: List[String] = List(),
    eventRepository: ActorRef,
    eventProducer: ActorRef)(implicit
    actorMaterializer: ActorMaterializer,
    executionContext: ExecutionContext) =
    Props(
      new TopicManager(
        pollInterval,
        includeInternalTopics,
        whitelistTopics,
        blacklistTopics,
        eventRepository,
        eventProducer))

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
  eventProducer: ActorRef)(implicit
  actorMaterializer: ActorMaterializer,
  val executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import TopicManager._
  import no.sysco.middleware.kafka.steward.collector.internal.OriginRepository._
  import no.sysco.middleware.kafka.steward.collector.metrics.Metrics._

  var topics: Map[String, Topic] = Map()

  implicit val timeout: Timeout = 10 seconds

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
    log.info(s"Handling {} topics collected.", topicsCollected.names.size)
    evaluateCurrentTopics(topics.keys.toList, topicsCollected.names)
    evaluateTopicsCollected(topicsCollected.names)
  }

  @tailrec
  private def evaluateCurrentTopics(currentNames: List[String], names: List[String]): Unit = {
    currentNames match {
      case Nil =>
      case name :: ns =>
        if (!names.contains(name)) {
          Stats.record(
            List(topicTypeTag, deletedOperationTypeTag),
            Measurement.double(totalMessageProducedMeasure, 1))
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
      if (topics.keys.exists(_.equals(name))) {
        eventRepository ! DescribeTopic(name)
      } else {
        Stats.record(
          List(topicTypeTag, createdOperationTypeTag),
          Measurement.double(totalMessageProducedMeasure, 1))
        eventProducer ! TopicEvent(name, TopicEvent.Event.TopicCreated(TopicCreated()))
      }
      evaluateTopicsCollected(names)
  }

  /**
   * Try to describe topic.
   * If internal topics are excluded and described topic is internal, method will exit.
   *
   * @param topicDescribed TopicDescribed information
   */
  def handleTopicDescribed(topicDescribed: TopicDescribed): Unit =
    topicDescribed.topicAndDescription match {
      case (topicName: String, topicDescription: TopicDescription) =>
        log.info("Handling topic {} described.", topicName)

        if (!includeInternalTopics) {
          if (topicDescription.internal) {
            log.warning("Internal topic excluded: {}", topicName)
            return
          }
        }

        topics.get(topicName) match {
          case None =>
            val configFuture =
              (eventRepository ? DescribeConfig(OriginRepository.ResourceType.Topic, topicName)).mapTo[ConfigDescribed]
            configFuture onComplete {
              case Success(configDescribed) =>
                Stats.record(
                  List(topicTypeTag, createdOperationTypeTag),
                  Measurement.double(totalMessageProducedMeasure, 1))
                eventProducer !
                  TopicEvent(topicName, TopicEvent.Event.TopicUpdated(toPb(topicDescription, configDescribed.config)))
              case Failure(t) => log.error(t, "Error querying config")
            }
          case Some(current) =>
            val configFuture =
              (eventRepository ? DescribeConfig(OriginRepository.ResourceType.Topic, topicName)).mapTo[ConfigDescribed]
            configFuture onComplete {
              case Success(configDescribed) =>
                if (!current.equals(Topic(topicName, topicDescription, configDescribed.config))) {
                  Stats.record(
                    List(topicTypeTag, updatedOperationTypeTag),
                    Measurement.double(totalMessageProducedMeasure, 1))
                  eventProducer !
                    TopicEvent(topicName, TopicEvent.Event.TopicUpdated(toPb(topicDescription, configDescribed.config)))
                }
              case Failure(t) => log.error(t, "Error querying config")
            }
        }
    }

  def handleTopicEvent(topicEvent: TopicEvent): Unit = {
    log.info("Handling topic {} event.", topicEvent.name)
    topicEvent.event match {
      case event if event.isTopicCreated =>
        Stats.record(
          List(topicTypeTag, createdOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
        eventRepository ! DescribeTopic(topicEvent.name)
      case event if event.isTopicUpdated =>
        Stats.record(
          List(topicTypeTag, updatedOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
            val topicDescription = fromPb(topicEvent.name, event.topicUpdated.get.topicDescription.get)
            val config = fromPb(event.topicUpdated.get.config)
            topics = topics + (topicEvent.name -> Topic(topicEvent.name, topicDescription, config))
      case event if event.isTopicDeleted =>
        Stats.record(
          List(topicTypeTag, deletedOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
          topics = topics - topicEvent.name
    }
  }

  def handleListTopics(): Unit =
    sender() ! Topics(topics.values.toList)
}
