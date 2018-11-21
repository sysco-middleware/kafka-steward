package no.sysco.middleware.kafka.steward.collector.topic.infra

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.topic.core.model._
import no.sysco.middleware.kafka.steward.collector.topic.infra.UpstreamSource.{ DescribeTopics, ListTopics }
import org.apache.kafka.clients.admin.{ AdminClient, TopicDescription }
import org.apache.kafka.common.config.ConfigResource

import scala.collection.JavaConverters._

object UpstreamSource {

  def props(adminClient: AdminClient, topicsRef: ActorRef): Props = Props(new UpstreamSource(adminClient, topicsRef))

  case class ListTopics()

  case class DescribeTopics(topics: Set[String])

}

class UpstreamSource(adminClient: AdminClient, topicsRef: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = {
    case ListTopics() => handleListTopics()
    case describeTopics: DescribeTopics => handleDescribeTopics(describeTopics)
  }

  private def handleListTopics(): Unit =
    adminClient.listTopics().names().thenApply { topics =>
      val topicsCollected = topics.asScala
      topicsRef ! TopicsCollected(topicsCollected.toList)
      self ! DescribeTopics(topicsCollected.toSet)
    }

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
              Config(entry._2.entries().asScala.toList.map { entry =>
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
