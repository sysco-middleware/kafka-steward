package no.sysco.middleware.kafka.steward.collector.topic.infra

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.topic.core.model._
import no.sysco.middleware.kafka.steward.collector.topic.infra.UpstreamSource.{ DescribeTopics, ListTopics }
import org.apache.kafka.clients.admin.{ AdminClient, ListTopicsOptions, TopicDescription }
import org.apache.kafka.common.config.ConfigResource

import scala.collection.JavaConverters._

object UpstreamSource {

  def props(
    adminClient: AdminClient,
    topicsRef: ActorRef,
    pollFrequency: Duration = Duration.ofSeconds(60),
    includeInternal: Boolean = false,
    whitelist: Set[String] = Set(),
    blacklist: Set[String] = Set()): Props =
    Props(new UpstreamSource(adminClient, topicsRef, pollFrequency: Duration, includeInternal, whitelist, blacklist))

  case class ListTopics()

  case class DescribeTopics(topics: Set[String])

}

class UpstreamSource(
  adminClient: AdminClient,
  topicsRef: ActorRef,
  pollFrequency: Duration,
  includeInternal: Boolean,
  whitelist: Set[String],
  blacklist: Set[String])
  extends Actor with ActorLogging {

  private val scheduler = context.system.scheduler

  override def receive: Receive = {
    case ListTopics() => handleListTopics()
    case describeTopics: DescribeTopics => handleDescribeTopics(describeTopics)
  }

  /**
   * List topics from a Kafka Cluster, pre-process the initial list with white/black lists, and
   * forward list for further process.
   */
  private def handleListTopics(): Unit =
    adminClient.listTopics(new ListTopicsOptions().listInternal(includeInternal))
      .names()
      .thenApply { topics =>
        // Pre-process list of topics
        var topicsCollected = topics.asScala
        if (whitelist.nonEmpty) topicsCollected = topicsCollected.intersect(whitelist)
        if (blacklist.nonEmpty) topicsCollected = topicsCollected.diff(blacklist)
        // Send topics to further processing
        topicsRef ! TopicsCollected(topicsCollected.toList)
        self ! DescribeTopics(topicsCollected.toSet)
        // Schedule next iteration
        scheduler.scheduleOnce(pollFrequency, self, ListTopics(), context.dispatcher, self)
      }

  /**
   * Collect description and configuration for topics.
   *
   * @param describeTopics List of topics to describe.
   */
  private def handleDescribeTopics(describeTopics: DescribeTopics): Unit = {
    val topicDescriptionsFuture = adminClient.describeTopics(describeTopics.topics.asJava).all()
    val topicConfigsFuture = adminClient.describeConfigs(describeTopics.topics.map {
      topic => new ConfigResource(ConfigResource.Type.TOPIC, topic)
    }.asJava).all()

    topicDescriptionsFuture.thenApply { topicDescriptions =>
      topicConfigsFuture.thenApply { topicConfigs =>
        processTopicDescribed(
          topicDescriptions.asScala.toList,
          topicConfigs.asScala.toList.map { entry =>
            (
              entry._1.name(),
              Config(
                entry._2.entries().asScala.toList.map { entry =>
                  (entry.name(), entry.value())
                }.toMap))
          }.toMap)
      }
    }
  }

  private def processTopicDescribed(
    topicDescriptions: List[(String, TopicDescription)],
    topicConfigs: Map[String, Config]): Unit = {
    topicDescriptions match {
      case Nil =>
      case (name, description) :: tail =>
        topicsRef !
          TopicDescribed(
            name,
            Topic(
              name,
              description.partitions().asScala.map { tpi =>
                Partition(
                  tpi.partition(),
                  tpi.replicas().asScala.map { r =>
                    Replica(BrokerId(r.id()))
                  }.toSet)
              }.toSet,
              topicConfigs(name),
              description.isInternal))
        processTopicDescribed(tail, topicConfigs)
    }
  }
}
